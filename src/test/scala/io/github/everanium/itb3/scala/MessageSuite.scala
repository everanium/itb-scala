// Single Message round trip across every shipped cipher profile at
// small (4 KiB) and medium (256 KiB) payloads. The blob-only profile
// has no cipher surface and is exercised in ErrorsSuite instead.

package io.github.everanium.itb3.scala

import scala.util.Using

object MessageSuite:
  /** Deterministic non-trivial payload (xorshift fill). */
  def payload(n: Int, seed: Long): Array[Byte] =
    val buf = new Array[Byte](n)
    var x = seed | 1L
    var i = 0
    while i < n do
      x ^= x << 13
      x ^= x >>> 7
      x ^= x << 17
      buf(i) = x.toByte
      i += 1
    buf

class MessageSuite extends ItbSuite:

  private val profiles = Seq(
    "streaming-aead-triple-mac-v1",
    "streaming-noaead-triple-v1",
    "singlemsg-triple-mac-v1",
    "singlemsg-triple-nomac-v1",
    "streaming-aead-triple-mac-mixed-v1",
    "streaming-noaead-triple-mixed-v1",
    "singlemsg-triple-mac-mixed-v1",
    "singlemsg-triple-nomac-mixed-v1"
  )

  test("message round trip on every shipped profile") {
    for profile <- profiles do
      Using.resource(ok(Pipeline.init(profile))) { sender =>
        Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
          for size <- Seq(4 * 1024, 256 * 1024) do
            val plain = MessageSuite.payload(size, size.toLong)
            val wire = ok(sender.encryptMessage(plain))
            val back = ok(receiver.decryptMessage(wire))
            assert(back.sameElements(plain), s"round trip mismatch: $profile @ $size")
        }
      }
  }
