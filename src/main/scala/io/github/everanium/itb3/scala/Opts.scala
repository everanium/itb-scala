// Immutable options builder mirroring the Java binding's Opts.
//
// The builder performs no validation — every key and value is passed
// through to Go verbatim; libitb3 rejects unknown keys or bad values
// with a diagnostic surfaced via ItbError. Primitive / MAC / cipher /
// palette names are opaque strings.

package io.github.everanium.itb3.scala

import io.github.everanium.itb3.Opts as JOpts

/** Immutable, chainable options for [[Pipeline.init]]. An empty
  * value renders the empty query (pure profile defaults). Profile
  * records for [[Pipeline.register]] are built with [[Profile]]. Each
  * `with*` call returns a new value; sharing a prefix between two
  * configurations is safe.
  */
final case class Opts(pairs: Vector[(String, String)] = Vector.empty):

  /** Escape hatch appending a raw `key=value` pair. Covers every key
    * the Go side accepts.
    */
  def withRaw(key: String, value: String): Opts = copy(pairs :+ (key -> value))

  /** Hex-encodes the parallax master override (`pm`). */
  def withPermMaster(master: Array[Byte]): Opts = withRaw("pm", Opts.hex(master))

  /** Hex-encodes the wrapper master override (`wm`). */
  def withWrapMaster(master: Array[Byte]): Opts = withRaw("wm", Opts.hex(master))

  def withParallax(on: Boolean): Opts = withRaw("withParallax", on.toString)

  def withWrapper(on: Boolean): Opts = withRaw("withWrapper", on.toString)

  def withMaxWorkers(n: Long): Opts = withRaw("maxWorkers", n.toString)

  def withNonceBits(n: Long): Opts = withRaw("nonceBits", n.toString)

  def withBarrierFill(n: Long): Opts = withRaw("barrierFill", n.toString)

  def withChunkSize(n: Long): Opts = withRaw("chunkSize", n.toString)

  def withKeyBits(n: Long): Opts = withRaw("keyBits", n.toString)

  def withParallaxSegmentSize(n: Long): Opts = withRaw("parallaxSegmentSize", n.toString)

  def withMacName(name: String): Opts = withRaw("macName", name)

  def withInnerHash(name: String): Opts = withRaw("innerHash", name)

  /** Comma-joins an 8-slot per-call inner-hash constellation into the
    * `innerHashes` opts key. Parallel to the Go-side
    * `Opts.MixedHashes [8]string` per-call override; slot ordering is
    * `[noise, lock, data1, data2, data3, start1, start2, start3]`.
    *
    * Fail-fast validation surfaces at Init on the Go side; a typo'd
    * slot or width mismatch surfaces with an error naming the
    * offending slot. When both this and [[withInnerHash]] are set,
    * the mixed override wins on the Go side.
    */
  def withInnerHashes(names: String*): Opts =
    withRaw("innerHashes", names.mkString(","))

  def withOuterCipher(name: String): Opts = withRaw("outerCipher", name)

  /** Comma-joins the palette names (`parallaxPalette`). */
  def withParallaxPalette(names: String*): Opts =
    withRaw("parallaxPalette", names.mkString(","))

  /** Replays the accumulated pairs into a fresh Java builder; the
    * Java side owns query rendering and percent-encoding.
    */
  private[scala] def toJava: JOpts =
    pairs.foldLeft(new JOpts()) { case (o, (k, v)) => o.withRaw(k, v) }

object Opts:
  /** The empty options value (pure profile defaults). */
  val empty: Opts = Opts()

  private def hex(bytes: Array[Byte]): String =
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xff}%02x"))
    sb.toString
