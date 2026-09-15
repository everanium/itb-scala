// Explicit write / end / read round trip with pathological batch
// sizes (17-byte feed, 23-byte drain) across multiple chunks.

package io.github.everanium.itb3.scala

import java.io.ByteArrayOutputStream

import scala.util.Using

class StreamIncrementalSuite extends ItbSuite:

  /** Feeds `src` in 17-byte writes, ends, drains in 23-byte reads. */
  private def pumpTiny(
      write: Array[Byte] => Either[ItbError, Unit],
      end: () => Either[ItbError, Unit],
      read: Array[Byte] => Either[ItbError, ReadResult],
      src: Array[Byte]
  ): Array[Byte] =
    src.grouped(17).foreach(chunk => ok(write(chunk)))
    ok(end())
    val spool = new ByteArrayOutputStream()
    val buf = new Array[Byte](23)
    var done = false
    while !done do
      val r = ok(read(buf))
      spool.write(buf, 0, r.count)
      done = r.finished
    spool.toByteArray

  test("incremental tiny batches") {
    // Small chunk size so the 64 KiB payload spans many chunks.
    val opts = Opts.empty.withChunkSize(4096)
    Using.resource(ok(Pipeline.init("streaming-aead-triple-mac-v1", opts))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) {
        receiver =>
          val plain = Array.tabulate(65_536)(i => (i % 241).toByte)

          val wire = Using.resource(ok(sender.beginEncryptStream())) { s =>
            pumpTiny(s.write, () => s.end(), s.read, plain)
          }
          assert(wire.nonEmpty)

          val back = Using.resource(ok(receiver.beginDecryptStream())) { s =>
            pumpTiny(s.write, () => s.end(), s.read, wire)
          }
          assert(back.sameElements(plain))
      }
    }
  }
