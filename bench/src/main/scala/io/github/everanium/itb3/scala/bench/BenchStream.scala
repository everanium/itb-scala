// Stream-pump throughput vs plaintext size (Streaming Non-AEAD
// profile) at 1 MiB / 16 MiB / 64 MiB.

package io.github.everanium.itb3.scala.bench

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.util.Using

import io.github.everanium.itb3.scala.Pipeline
object BenchStream:

  def run(): Unit =
    val profile = BenchUtil.profileName("streaming-noaead-triple-v1")
    Using.resource(
      Pipeline.init(profile, BenchUtil.buildOpts).fold(e => throw e, identity)
    ) { pipe =>
      BenchUtil.header()
      for size <- BenchUtil.Sizes do
        val plain = BenchUtil.payload(size)
        BenchUtil.benchCase("stream_pump", size) {
          val wire = new ByteArrayOutputStream(size + size / 4 + 131_072)
          pipe
            .encryptStreamPump(new ByteArrayInputStream(plain), wire)
            .fold(e => throw e, _ => ())
        }
        // Pre-encrypt one wire outside the decrypt timing loop.
        val setupWire = new ByteArrayOutputStream(size + size / 4 + 131_072)
        pipe
          .encryptStreamPump(new ByteArrayInputStream(plain), setupWire)
          .fold(e => throw e, _ => ())
        val decWire = setupWire.toByteArray
        BenchUtil.benchCase("stream_pump-dec", size) {
          val out = new ByteArrayOutputStream(size + 131_072)
          pipe
            .decryptStreamPump(new ByteArrayInputStream(decWire), out)
            .fold(e => throw e, _ => ())
        }
    }
