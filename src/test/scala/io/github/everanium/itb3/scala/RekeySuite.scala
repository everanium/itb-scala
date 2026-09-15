// Init -> rekey -> load receiver from the rotated blob -> round
// trip.

package io.github.everanium.itb3.scala

import scala.util.Using

class RekeySuite extends ItbSuite:

  test("rekey round trip") {
    Using.resource(ok(Pipeline.init("singlemsg-triple-mac-v1"))) { sender =>
      val blobBefore = ok(sender.save())

      val perm = Array.fill(32)(0x11.toByte)
      val wrap = Array.fill(32)(0x22.toByte)
      ok(sender.rekey(perm, wrap))
      assert(!ok(sender.save()).sameElements(blobBefore))

      Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
        val plain = "post-rekey payload".getBytes("UTF-8")
        val wire = ok(sender.encryptMessage(plain))
        assert(ok(receiver.decryptMessage(wire)).sameElements(plain))
      }
    }
  }
