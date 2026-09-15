// Init -> save -> load -> encryptMessage -> decryptMessage round
// trip, plus the shared suite base used by every test file.

package io.github.everanium.itb3.scala

import scala.util.Using

/** Base class for the binding test suites: unwraps `Either` results
  * with a diagnostic failure message.
  */
abstract class ItbSuite extends munit.FunSuite:

  /** Unwraps a success or fails the test with the error rendering. */
  def ok[A](result: Either[ItbError, A]): A =
    result.fold(e => fail(s"unexpected ITB error: ${e.getMessage}"), identity)

  /** Unwraps an expected failure or fails the test. */
  def err[A](result: Either[ItbError, A]): ItbError =
    result.fold(identity, a => fail(s"expected an ITB error, got success: $a"))

class SmokeSuite extends ItbSuite:

  test("library version is non-empty") {
    assert(Runtime.version.nonEmpty)
    assertEquals(Runtime.BindingVersion, "0.5.1")
  }

  test("smoke round trip") {
    Using.resource(ok(Pipeline.init("singlemsg-triple-mac-v1"))) { sender =>
      assert(ok(sender.save()).nonEmpty)
      Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
        val plain = "smoke round-trip payload".getBytes("UTF-8")
        val wire = ok(sender.encryptMessage(plain))
        assert(!wire.sameElements(plain))
        val back = ok(receiver.decryptMessage(wire))
        assert(back.sameElements(plain))
      }
    }
  }
