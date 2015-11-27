
import com.datastax.spark.connector._
import com.google.gson.Gson
import org.apache.spark.{SparkConf, SparkContext}


/**
 * spark-submit --master local[2] --class SwissSimInsert \
 * sparssandra-1.0.0-SNAPSHOT.jar swisssim molecules cassandra1.fri.lan smileAndFinderprint.smi smileAndDetail.smi
 */
object SwissSimInsert {

  // scalastyle:off println

  private val gson = new Gson()

  def main(args: Array[String]) {
    // Process program arguments and set properties
    if (args.length < 5) {
      println("Usage: " + this.getClass.getSimpleName +
        "<keyspace> <table> <cassandraSeed> <file1> <file2>")
      System.exit(1)
    }
    val Array(keyspace: String, table: String, cassandraSeed: String,
      file1: String, file2: String ) = args

    println("Initializing Streaming Spark Context...")
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.cassandra.connection.host", cassandraSeed)

    val sc = new SparkContext(conf)

    val smileAndFingerPrint = sc.textFile(file1).map(line => {
      val v = line.split("\\s+"); (v(0).trim, HexBytesUtil.hex2bytes(v(1).trim))
    } )

    val smileAndDetail = sc.textFile(file2).map(line => {
      val idx = line.split("\\s+")

      val smile = idx(0).trim
      // TODO: this must be put in a UDT at some point...
      val detail = idx.drop(1).mkString(" ")
      (smile, detail)
    })

    val join = smileAndFingerPrint.join(smileAndDetail).map(r =>
      Molecule(r._2._1, r._1, r._2._2))

//    join.collect().foreach(m =>
//      println(m.smile + ",-," + HexBytesUtil.bytes2hex(m.fingerprint) + ",-," + m.details))

    join.saveToCassandra(keyspace, table)

  }

}
