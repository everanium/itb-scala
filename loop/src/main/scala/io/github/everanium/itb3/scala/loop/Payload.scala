// Plaintext content: the payload modes, the seeded per-worker
// generator, and the buffer fill from the operating-system CSPRNG.

package io.github.everanium.itb3.scala.loop

import java.security.SecureRandom
import java.util.Arrays

/** Payload mode selector values for the --payload-mode flag.
  *
  *   - Fixed: one CSPRNG-generated buffer per worker, held unchanged
  *     for the whole run (the default).
  *   - Rotating: the buffer is regenerated before every iteration, so
  *     no two encrypt calls see the same plaintext.
  *   - PatternZero / PatternFf: degenerate constant fills (all 0x00 /
  *     all 0xFF) probing minimum-entropy plaintext handling.
  *   - PatternAscii: a repeating 'A'..'Z' ramp probing low-entropy
  *     structured text.
  */
enum PayloadMode(val label: String):
  case Fixed extends PayloadMode("fixed")
  case Rotating extends PayloadMode("rotating")
  case PatternZero extends PayloadMode("pattern-zero")
  case PatternFf extends PayloadMode("pattern-ff")
  case PatternAscii extends PayloadMode("pattern-ascii")

object PayloadMode:
  def parse(s: String): Option[PayloadMode] = PayloadMode.values.find(_.label == s)

/** The deterministic per-worker generator's state. Scala has no
  * by-reference scalar parameter, so the splitmix64 state lives in this
  * one-field holder that the worker owns and the generator advances in
  * place.
  */
final class Rng(var state: Long)

object Payload:

  /** The operating-system CSPRNG, one instance per calling thread so
    * concurrent payload refills never serialise on one provider.
    */
  private val csprng: ThreadLocal[SecureRandom] =
    ThreadLocal.withInitial(() => SecureRandom())

  /** Seeded plaintext. The seed makes plaintext content reproducible so
    * a failing iteration can be replayed with the same bytes; it
    * governs nothing else — pipeline keys, nonces and masters stay
    * CSPRNG-drawn, so a seeded run is a reproduction aid and never a
    * security test. Each worker's stream is domain-separated by its id
    * so seeded workers still hold pairwise-distinct buffers under the
    * fixed and rotating modes. The generator is splitmix64: a few lines
    * in any language, which is why it is the one every binding uses.
    */
  def seedWorker(seed: Long, workerId: Int): Long = seed + workerId + 1

  private def splitmix64(rng: Rng): Long =
    rng.state += 0x9e3779b97f4a7c15L
    var z = rng.state
    z = (z ^ (z >>> 30)) * 0xbf58476d1ce4e5b9L
    z = (z ^ (z >>> 27)) * 0x94d049bb133111ebL
    z ^ (z >>> 31)

  /** Fills `buf` from the operating-system CSPRNG.
    *
    * Scala-specific. The JDK's default `SecureRandom` provider on this
    * platform draws from `/dev/urandom` through a file descriptor
    * rather than through the glibc `getrandom` entry, and the platform
    * offers no supported way for a JVM source set to reach that entry —
    * a native declaration of its own is exactly the reach past the
    * binding the utility must not make. One draw still happens per
    * fill; it is observable as a read on that descriptor rather than as
    * a `getrandom` call.
    */
  def fillRandom(buf: Array[Byte]): Boolean =
    csprng.get().nextBytes(buf)
    true

  /** Writes one plaintext buffer according to the payload mode. The
    * fixed and rotating modes draw from the seeded generator when the
    * run is seeded and from the OS CSPRNG otherwise; the pattern modes
    * are deterministic regardless of the seed. False when the CSPRNG
    * fails.
    */
  def fill(mode: PayloadMode, seeded: Boolean, rng: Rng, buf: Array[Byte]): Boolean =
    mode match
      case PayloadMode.Fixed | PayloadMode.Rotating =>
        if !seeded then fillRandom(buf)
        else
          var i = 0
          while i < buf.length do
            val v = splitmix64(rng)
            val n = math.min(8, buf.length - i)
            var k = 0
            while k < n do
              buf(i + k) = (v >>> (8 * k)).toByte
              k += 1
            i += 8
          true
      case PayloadMode.PatternZero =>
        Arrays.fill(buf, 0.toByte)
        true
      case PayloadMode.PatternFf =>
        Arrays.fill(buf, 0xff.toByte)
        true
      case PayloadMode.PatternAscii =>
        var i = 0
        while i < buf.length do
          buf(i) = ('A'.toInt + (i % 26)).toByte
          i += 1
        true
