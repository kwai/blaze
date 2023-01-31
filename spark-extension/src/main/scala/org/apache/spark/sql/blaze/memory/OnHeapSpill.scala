/*
 * Copyright 2022 The Blaze Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.blaze.memory

import java.io.ByteArrayInputStream
import java.nio.channels.WritableByteChannel
import java.nio.ByteBuffer
import java.nio.channels.Channels
import java.nio.channels.ReadableByteChannel

import org.apache.commons.io.output.ByteArrayOutputStream

case class OnHeapSpill(id: Int, capacity: Long) {

  private var stream: Either[
    (ByteArrayOutputStream, WritableByteChannel),
    (ByteArrayInputStream, ReadableByteChannel)] = Left({
    val out = new ByteArrayOutputStream()
    val outChannel = Channels.newChannel(out)
    (out, outChannel)
  })

  def isCompleted: Boolean = stream.isRight

  def write(buf: ByteBuffer): Unit = {
    val (_, outChannel) = stream.left.get
    while (buf.hasRemaining && outChannel.write(buf) >= 0) {}
  }

  def read(buf: ByteBuffer): Int = {
    val (_, inChannel) = stream.right.get
    while (buf.hasRemaining && inChannel.read(buf) >= 0) {}
    buf.position()
  }

  def complete(): Unit = {
    val out = stream.left.get._1
    val in = new ByteArrayInputStream(out.toByteArray)
    val inChannel = Channels.newChannel(in)
    stream = Right((in, inChannel))
  }
}
