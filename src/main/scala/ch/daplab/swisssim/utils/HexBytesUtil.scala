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

package ch.daplab.swisssim.utils

import java.nio.{ByteOrder, LongBuffer, ByteBuffer}


object HexBytesUtil {

  def hex2bytes(hex: String): Array[Byte] = {
    hex.replaceAll("[^0-9A-Fa-f]", "").sliding(2, 2).toArray.map(Integer.parseInt(_, 16).toByte)
  }

  def bytes2hex(bytes: Array[Byte], sep: Option[String] = None): String = {
    sep match {
      case None => bytes.map("%02x".format(_)).mkString
      case _ => bytes.map("%02x".format(_)).mkString(sep.get)
    }
  }

  def bytes2hex(bytes: ByteBuffer): String = {
    bytes2hex(bytes.array())
  }

  def longBufferToLong(longs: LongBuffer): Array[Long] = {
    (0 to longs.limit() - 1).map(longs.get(_)).to[Array]
  }

  def byteArrayToLongArray(ba: Array[Byte]): Array[Long] = {
    ba.sliding(8, 8).map(ByteBuffer.wrap(_)
//      .order(ByteOrder.BIG_ENDIAN)
      .getLong).toArray
  }

  def main(args: Array[String]) {

    val input = "0001801a018002082100110821001108"
    val v1 = hex2bytes(input)
    val v2 = byteArrayToLongArray(v1)
    val ba = ByteBuffer.wrap(v1).asLongBuffer()
    val v3 = longBufferToLong(ba)

    println("test")
  }

  def tanimoto(query: Array[Long], molecule: Array[Long]): Double = {

    val (andCardinality: Double, orCardinality: Double) =
      query.zip(molecule).foldLeft((0.0, 0.0))
      { case ((and, or), (b, x)) =>
        (and + java.lang.Long.bitCount(x & b),
          or + java.lang.Long.bitCount(x | b))
      }

    //        query.zip(moleculeArray).foreach((p: (Long, Long)) => {
    //          val b = p._1
    //          val x = p._2
    //          andCardinality += java.lang.Long.bitCount(x & b)
    //          orCardinality += java.lang.Long.bitCount(x | b)
    //        })

    andCardinality / orCardinality
  }

}