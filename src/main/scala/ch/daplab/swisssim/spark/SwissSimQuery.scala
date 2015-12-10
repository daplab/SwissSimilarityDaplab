// Copyright (C) 2015 the original author or authors.
// See the LICENCE.txt file distributed with this work for additional
// information regarding copyright ownership.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package ch.daplab.swisssim.spark

import java.nio.{LongBuffer, ByteBuffer}

import ch.daplab.swisssim.dto.Molecule
import ch.daplab.swisssim.utils.HexBytesUtil
import com.datastax.spark.connector._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * spark-submit --master "yarn" --num-executors 12 --class SwissSimQuery
  * sparssandra-1.0.0-SNAPSHOT.jar swisssim molecules cassandra1.fri.lan
  * 0001801a018002082100110810b00610001c106000004020030098020000110806100
  * 100400048e00100a00c608188000200980702600201816600c1020c000c0800000804
  * 00800802e0200800022801c010008018008138218000418970041800086000c008014
  * 020048000000400880400420420c6000860260000614306a1
  * 10
  */
object SwissSimQuery {

  // scalastyle:off println

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 5) {
      println("Usage: " + this.getClass.getSimpleName +
        "<keyspace> <table> <cassandraSeed> <hexByte> <numberToReturn>")
      System.exit(1)
    }
    val Array(keyspace: String, table: String, cassandraSeed: String,
      userInputStr: String, numberToReturnStr: String ) = args

    // ByteBuffer and LongBuffer are not serializable...
    val userInput = HexBytesUtil.byteArrayToLongArray(HexBytesUtil.hex2bytes(userInputStr))

    val numberToReturn: Int = numberToReturnStr.toInt

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.cassandra.connection.host", cassandraSeed)

    val sc = new SparkContext(conf)

    val fingerprints = sc.cassandraTable(keyspace, table).select("fingerprint")
      .map(r => r.getBytes("fingerprint"))

//    val fp2 = sc.cassandraTable(keyspace, table).select("fingerprint")
//      .map(r => r.getBytes("fingerprint")).repartition(12)

    fingerprints.cache()

    val tanimotorrd: RDD[(Double, Array[Byte])] = fingerprints
      .map(b => { val ba = b.array()
        val t = (tanimoto(userInput, ba), ba);
        t
      })
      .filter( t => t._1 >= 0.8)

      // TODO: don't sort by key here, but keep numberToReturn in a priority
      // queue and return the queue only, i.e. void shuffle and
      // http://docs.guava-libraries.googlecode.com/git/javadoc/com/google/common/collect/MinMaxPriorityQueue.html
      .sortByKey(false)

      .mapPartitions(p => {
        var i: Long = 0;
        p.filter(o => {
          i += 1
          if (i <= numberToReturn) {
            true
          } else {
            false
          }
        })
      })

    println(tanimotorrd.toDebugString)
    println(tanimotorrd.count)

    val topN = tanimotorrd.take(numberToReturn)

    topN.foreach{case (similarity, fingerprint) =>
      println("(" + similarity + "," + HexBytesUtil.bytes2hex(fingerprint) + ")")}

    val moleculerrd = sc.parallelize(topN).map(r => (r._2, r._1))
      .joinWithCassandraTable(keyspace, table).map(r =>
      (new Molecule(r._1._1, r._2.getString("smile"), r._2.getString("details")), r._1._2))

    moleculerrd.collect().foreach(r =>
      println(HexBytesUtil.bytes2hex(r._1.fingerprint) + ", " +
        r._1.smile + ", " + r._2 + ", " + r._1.details)
    )

  }

    def tanimoto(query: Array[Long], molecule: Array[Byte]): Double = {

      var andCardinality = 0.0
      var orCardinality = 0.0

      val moleculeArray = HexBytesUtil.byteArrayToLongArray(molecule)

      query.zip(moleculeArray).foreach((p: (Long, Long)) => {
        val b = p._1
        val x = p._2
        andCardinality += java.lang.Long.bitCount(x & b)
        orCardinality += java.lang.Long.bitCount(x | b)
      })

      andCardinality / orCardinality
    }

}
