// Round trips through the InputStream / OutputStream pumps and the
// chunk-iterator adapters on a Streaming AEAD profile.

package io.github.everanium.itb3.scala

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import scala.util.Using

class StreamPumpSuite extends ItbSuite:

  private val profile = "streaming-aead-triple-mac-v1"

  test("pump round trip 1 MiB") {
    Using.resource(ok(Pipeline.init(profile))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
        val plain = Array.tabulate(1 << 20)(i => (i % 251).toByte)

        val wire = new ByteArrayOutputStream()
        ok(sender.encryptStreamPump(new ByteArrayInputStream(plain), wire))
        assert(wire.size > 0)

        val back = new ByteArrayOutputStream()
        ok(receiver.decryptStreamPump(new ByteArrayInputStream(wire.toByteArray), back))
        assert(back.toByteArray.sameElements(plain))
      }
    }
  }

  test("pump matches one-shot") {
    Using.resource(ok(Pipeline.init(profile))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
        val plain = Array.tabulate(65_536)(i => (i % 199).toByte)
        val wire = ok(sender.encryptStreamOneShot(plain))

        val back = new ByteArrayOutputStream()
        ok(receiver.decryptStreamPump(new ByteArrayInputStream(wire), back))
        assert(back.toByteArray.sameElements(plain))

        val back2 = ok(receiver.decryptStreamOneShot(wire))
        assert(back2.sameElements(plain))
      }
    }
  }

  test("iterator transform round trip") {
    Using.resource(ok(Pipeline.init(profile))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
        val plain = MessageSuite.payload(300_000, 0x5eed)
        // Uneven input chunking (7001-byte slices) exercises the
        // feed / drain interleave.
        val wire = Using.resource(ok(sender.beginEncryptStream())) { session =>
          session.transform(plain.grouped(7001)).foldLeft(Array.emptyByteArray)(_ ++ _)
        }
        assert(wire.nonEmpty)
        val back = Using.resource(ok(receiver.beginDecryptStream())) { session =>
          session.toLazyList(wire.grouped(4099)).foldLeft(Array.emptyByteArray)(_ ++ _)
        }
        assert(back.sameElements(plain))
      }
    }
  }
