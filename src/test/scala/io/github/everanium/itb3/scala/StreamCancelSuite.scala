// Closing an encrypt session mid-flight cleans up and leaves the
// Pipeline usable.

package io.github.everanium.itb3.scala

import scala.util.Using

class StreamCancelSuite extends ItbSuite:

  test("close mid-flight then reuse pipeline") {
    Using.resource(ok(Pipeline.init("streaming-aead-triple-mac-v1"))) { sender =>
      Using.resource(ok(sender.beginEncryptStream())) { session =>
        val block = Array.fill(100_000)(0xa5.toByte)
        ok(session.write(block))
        // Closed here without end() — close cancels and frees the
        // session; the test passing (process not hanging) is the
        // assertion.
      }

      // The Pipeline stays usable after the cancelled session.
      Using.resource(ok(Pipeline.load(ok(sender.save())))) {
        receiver =>
          val plain = "after cancel".getBytes("UTF-8")
          val wire = ok(sender.encryptMessage(plain))
          assert(ok(receiver.decryptMessage(wire)).sameElements(plain))
      }
    }
  }
