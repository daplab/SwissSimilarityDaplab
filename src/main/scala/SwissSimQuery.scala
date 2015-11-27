
import com.google.gson.Gson
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import com.datastax.spark.connector._

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

  private val gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 5) {
      println("Usage: " + this.getClass.getSimpleName +
        "<keyspace> <table> <cassandraSeed> <hexByte> <numberToReturn>")
      System.exit(1)
    }
    val Array(keyspace: String, table: String, cassandraSeed: String,
      userInputStr: String, numberToReturnStr: String ) = args

    val userInput = HexBytesUtil.hex2bytes(userInputStr)
    val numberToReturn: Int = numberToReturnStr.toInt

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.cassandra.connection.host", cassandraSeed)

    val sc = new SparkContext(conf)

    val fingerprints = sc.cassandraTable(keyspace, table).map(_.get[Array[Byte]]("fingerprint"))
//    fingerprints.cache()

    val tanimotorrd: RDD[(Float, Array[Byte])] = fingerprints
      .map(b => {
        val t = (tanimoto(b, userInput), b); println(t); t
      })
      .filter( t => t._1 >= 0.8)
      .sortByKey(false)
      .mapPartitions(p => {
        var i: Long = 0;
        p.map(o => {
          i += 1
          if (i <= numberToReturn) {
            o
          } else {
            null
          }
        })
      }).filter(_ != null)

    println(tanimotorrd.count())

    val topN = tanimotorrd.take(numberToReturn)

    topN.foreach(b => println("(" + b._1 + "," + HexBytesUtil.bytes2hex(b._2) + ")"))

    val moleculerrd = sc.parallelize(topN).map(r => (r._2, r._1))
      .joinWithCassandraTable(keyspace, table).map(r =>
      (new Molecule(r._1._1, r._2.getString("smile"), r._2.getString("details")), r._1._2))

    moleculerrd.collect().foreach(r =>
      println(HexBytesUtil.bytes2hex(r._1.fingerprint) + ", " +
        r._1.smile + ", " + r._2 + ", " + r._1.details)
    )

  }

    def tanimoto(query: Array[Byte], database: Array[Byte]): Float = {

      var z = 0
      var z2 = 0

      query.zip(database).foreach((x: (Byte, Byte)) => {
        z += numberOfBitsSet((x._1 & x._2).toByte)
        z2 += numberOfBitsSet((x._1 | x._2).toByte)
      })

      (z: Float) / z2
    }

  def numberOfBitsSet(b: Byte) : Int = (0 to 7).map((i : Int) => (b >>> i) & 1).sum

}
