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

import java.util.UUID
import java.util.concurrent._

import ch.daplab.swisssim.dto.{QueryCache, UserRequest, Molecule, UserInput}
import ch.daplab.swisssim.utils.{MoleculeSimilarityComparator, HexBytesUtil}
import com.datastax.spark.connector._
import com.google.common.collect.MinMaxPriorityQueue
import com.google.common.collect.MinMaxPriorityQueue.Builder
import com.twitter.finagle.http.Method.{Post, Get}
import com.twitter.logging.Logging

import com.typesafe.config.{ConfigFactory, Config}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable
import scala.collection.JavaConverters._
import com.twitter.finagle.{Http, Service}
import com.twitter.finagle.http
import com.twitter.util.{Await, Future}
import org.json4s._
import org.json4s.jackson.JsonMethods._
import com.twitter.logging.Logger

// scala.App defines main function
object SwissSimEngine extends App {

  private val log = Logger.get(getClass)

  val DEFAULT_FINGERPRINT_LENGTH: Int = 128
  val DEFAULT_NUMBER_TO_RETURN: Int = 1000
  val DEFAULT_SIMILARITY_THRESHOLD: Double = 0.7

  // Configuration parameter read from application.conf in the classpath
  val CONF_CASSANDRA_KEYSPACE: String = "cassandra.keyspace"
  val CONF_CASSANDRA_TABLE: String = "cassandra.table"
  val CONF_CASSANDRA_TABLE_QUERY_CACHE: String = "cassandra.table_query_cache"
  val CONF_CASSANDRA_HOSTS: String = "cassandra.hosts"

  val CONF_REST_PORT: String = "rest.port"

  val config: Config = ConfigFactory.load()

  val keyspace = config.getString(CONF_CASSANDRA_KEYSPACE)
  val table = config.getString(CONF_CASSANDRA_TABLE)
  val tableQueryCache = config.getString(CONF_CASSANDRA_TABLE_QUERY_CACHE)
  val hosts = config.getString(CONF_CASSANDRA_HOSTS)
  val port = config.getInt(CONF_REST_PORT)

  // User requests from API will be queued in a liked blocking queue to be
  // exchanged between the web server and the Spark job
  val inputQueue: LinkedBlockingQueue[UserInput] = new LinkedBlockingQueue[UserInput]()

  // User requests are stored in a map while the spark query is running
  val requestMap: mutable.Map[UUID, UserInput] =
    new scala.collection.mutable.HashMap
  // Responses are also stored in a map by the spark query for retrieval from the API
  val responseMap: mutable.Map[UUID, String] =
    new scala.collection.mutable.HashMap

  // SparkContext initialization as lazy (i.e. the server will have
  // fast startup time
  lazy val sc = {
    val conf = new SparkConf().setAppName(this.getClass.getSimpleName)
      .set("spark.cassandra.connection.host", hosts)
    new SparkContext(conf)
  }

  // Cassandra key load is put in Spark cache for faster retrieval
  // i.e. the second query should be significantly faster
  lazy val fingerprints = sc.cassandraTable(keyspace, table).select("fingerprint")
    .map(r => r.getBytes("fingerprint").array())
    .cache()

  val scheduledExecutorService: ScheduledExecutorService =
    Executors.newSingleThreadScheduledExecutor()

  // Insert user request into the queue (to be processed by the spark job
  // as well as kept in map to return 202 when polling for the result
  def enqueue(userInput: UserInput): Unit = {
    requestMap += (userInput.requestId -> userInput)
    inputQueue.offer(userInput)
  }

  // URI mapping inside the web server. Will match the request id
  // from the URI directly
  val resultUri = "(/api/v1/result/)(.*)".r
  implicit val formats = DefaultFormats

  // Finagle web service definition
  val service = new Service[http.Request, http.Response] {

    def apply(req: http.Request): Future[http.Response] = {

      // some debugging (TODO: move to proper logging)
      log.debug("Got a request %s", req.uri)

      // The API defines:
      // - POST /api/v1/submit to insert a new job
      // - GET /api/v1/result/UUID to retrieve the result
      try {
        val r = req.method match {

          case Get => {
            req.uri match {
              case resultUri(result, requestId) => {
                log.info("The request has a request id %s", requestId)
                try {
                  val requestUUID = UUID.fromString(requestId)
                  requestMap.get(requestUUID) match {
                    case Some(request) => {
                      log.debug("The request exists")
                      responseMap.get(requestUUID) match {
                        case Some(response) => {
                          log.debug("The response exists")
                          val r = http.Response(req.version, http.Status.Ok)
                          r.setContentTypeJson()
                          r.setContentString(response)
                          r
                        }
                        case None => http.Response(req.version, http.Status.Accepted)
                      }
                    }
                    case None => http.Response(req.version, http.Status.NotFound)
                  }
                } catch {
                  case e: IllegalArgumentException =>
                    http.Response(req.version, http.Status.BadRequest)
                }
              }
              case "/ping" => {
                val r = http.Response(req.version, http.Status.Ok)
                r.setContentTypeJson()
                r.setContentString("{ \"pong\" }")
                r
              }
              case "/admin" => {
                val r = http.Response(req.version, http.Status.Ok)
                r
              }
              case _ => http.Response(req.version, http.Status.NotFound)
            }
          }
          case Post => {
            req.uri match {
              case "/api/v1/submit" => {
                val userInput: Option[UserInput] =
                try {
                  val tmpUserRequest = parse(req.contentString).extract[UserRequest]
                  val userRequest = new UserRequest(tmpUserRequest.fingerprint,
                    Math.min(tmpUserRequest.limit, DEFAULT_NUMBER_TO_RETURN),
                    Math.max(tmpUserRequest.threshold, DEFAULT_SIMILARITY_THRESHOLD)
                  )
                  val uuid = UUID.randomUUID
                  val fingerprint = HexBytesUtil.hex2bytes(userRequest.fingerprint)
                  if (fingerprint == null || fingerprint.length != DEFAULT_FINGERPRINT_LENGTH) {
                    throw new IllegalArgumentException("Wrong fingerprint length")
                  } else {
                    Some(UserInput(fingerprint, uuid, userRequest))
                  }
                } catch {
                  case e: Exception => {
                    log.error("Exception while parsing the request", e)
                    e.printStackTrace()
                    None
                  }
                }

                userInput match {
                  case Some(i) => {
                    log.info("User request, once parsed, is " +
                      i.userRequest.fingerprint + ", " + i.requestId)
                    enqueue(i)
                    val r = http.Response(req.version, http.Status.Created)
                    r.headerMap.add("Location", "/api/v1/result/" + i.requestId)
                    r.setContentString("{ \"requestId\": \"" + i.requestId + "\" }")
                    r
                  }
                  case None => http.Response(req.version, http.Status.BadRequest)
                }

              }
              case _ => http.Response(req.version, http.Status.NotFound)
            }
          }
          case _ => http.Response(req.version, http.Status.NotImplemented)
        }

        Future.value(
          r
        )

      } catch {
        case e: Exception => {
          log.error("Exception while parsing the request", e)
          e.printStackTrace()
          Future.value(
            http.Response(req.version, http.Status.InternalServerError)
          )
        }
      }
    }
  }

  object MoleculeSimilarityOrdering extends Ordering[(Double, Array[Byte])] {
    override def compare(x: (Double, Array[Byte]), y: (Double, Array[Byte])): Int =
      x._1 compareTo y._1
  }

  object QueryCacheOrdering extends Ordering[QueryCache] {
    override def compare(x: QueryCache, y: QueryCache): Int =
      x.similarity compareTo y.similarity
  }

  def checkCache(sc: SparkContext, userRequest: UserInput): Option[String] = {
    val cacherdd = sc.cassandraTable(keyspace, tableQueryCache)
      .where("query = ? and similarity > ?", userRequest.fingerprint,
        userRequest.userRequest.threshold).limit(userRequest.userRequest.limit)
      .map(r => new QueryCache(r.getBytes("query").array(),
        r.getDouble("similarity"), r.getBytes("fingerprint").array(), r.getString("smile"),
      r.getString("details")))

    if (cacherdd.isEmpty()) {
      return None
    } else {
      return Some(formatRDD(cacherdd,
        userRequest.userRequest.limit,
        userRequest.userRequest.threshold))
    }
  }

  def query(sc: SparkContext, userRequest: UserInput): Unit = {

    // scalastyle:off println

    log.info("Starting the heavy query for request %s (%s)",
      userRequest.requestId, userRequest.userRequest.fingerprint)

    val numberToReturn = Math.min(userRequest.userRequest.limit,
      DEFAULT_NUMBER_TO_RETURN)
    val similarityThreshold = Math.max(userRequest.userRequest.threshold,
      DEFAULT_SIMILARITY_THRESHOLD)

    val userInput = HexBytesUtil.byteArrayToLongArray(userRequest.fingerprint)

    val tanimotorrd: RDD[(Double, Array[Byte])] = fingerprints
      .map(b => {
        val ba = b.array
        (HexBytesUtil.tanimoto(userInput, HexBytesUtil.byteArrayToLongArray(ba)), ba)
      })
      .filter( t => t._1 >= DEFAULT_SIMILARITY_THRESHOLD)

    println(tanimotorrd.toDebugString)
    println(tanimotorrd.count)

    val topN = tanimotorrd.top(DEFAULT_NUMBER_TO_RETURN)(MoleculeSimilarityOrdering)

    topN.foreach {case (similarity, fingerprint) =>
      println("(" + similarity + "," + HexBytesUtil.bytes2hex(fingerprint) + ")")}

    val moleculerrd: RDD[QueryCache] = sc.parallelize(topN).map(r => (r._2, r._1))
      .joinWithCassandraTable(keyspace, table).map(r =>
      new QueryCache(userRequest.fingerprint, r._1._2, r._1._1,
        r._2.getString("smile"), r._2.getString("details")))

    try {
      moleculerrd.saveToCassandra(keyspace, tableQueryCache,
          SomeColumns("query", "similarity", "fingerprint", "smile", "details"))
    } catch {
      case e: Exception => {
        log.error("Exception while saving the cache to Cassandra", e)
        e.printStackTrace()
      }
    }

    val r = formatRDD(moleculerrd, numberToReturn, similarityThreshold)

    println("1 " + userRequest.requestId + ", " + r)

    responseMap += userRequest.requestId -> r

    scheduledExecutorService.schedule(
      new Runnable {
        val requestId = userRequest.requestId
        override def run(): Unit = {
          responseMap.remove(requestId)
          requestMap.remove(requestId)
        }
      }, 60, TimeUnit.MINUTES)

  }

  class InputListener extends Runnable with AutoCloseable {
    private var running = true;
    override def run(): Unit = {
      while (running) {
        try {
          val item = inputQueue.take()
          log.info("Got a new request, %s", item.requestId)
          checkCache(sc, item) match {
            case Some(cache) => {
              log.info("Cache found, %s", cache)
              responseMap += item.requestId -> cache
            }
            case None => query(sc, item)
            case _ => {
              log.error("Ouups...")
            }
          }
        } catch {
          case e: Exception => {
            log.error("Exception while processing the user request", e)
            e.printStackTrace()
          }
        }
      }
    }

    override def close(): Unit = {
      running = false
    }
  }

  // define number of requests processed in parallel
  val executor: ExecutorService = Executors.newFixedThreadPool(1)

  executor.submit(new InputListener)

  val server = Http.serve(":" + port, service)
  Await.ready(server)

  def formatRDD(rdd: RDD[QueryCache], limit: Int, threshold: Double ): String = {
    "[" + rdd.filter(_.similarity > threshold).top(limit)(QueryCacheOrdering).map(r =>
      "{ \"fingerprint\": \"" + HexBytesUtil.bytes2hex(r.fingerprint) + "\", " +
        "\"smile\": \"" + r.smile + "\", \"similarity\": " + r.similarity + ", \"details\": \"" +
        r.details + "\" }"
    ).mkString("\n") + "]"
  }
}
