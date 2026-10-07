// Size and duration parsing and the human renderings of sizes, rates
// and durations. Every rendering here is part of the output contract
// shared with the Go harness and the other bindings' loop utilities,
// so the formats are fixed to the character, not to taste.
//
// Scala-specific. Every formatter names Locale.ROOT explicitly. A
// format string that relies on the ambient default renders "1,5MB/s"
// the day someone runs the harness under a comma-decimal locale, and
// the output contract is byte-for-byte.

package io.github.everanium.itb3.scala.loop

import java.util.Locale

object Size:

  /** Suffix table for [[parseSize]], matched in order so the longer
    * spellings win over their prefixes.
    */
  private val SizeSuffixes: List[(String, Long)] = List(
    "KIB" -> (1L << 10),
    "KB" -> (1L << 10),
    "K" -> (1L << 10),
    "MIB" -> (1L << 20),
    "MB" -> (1L << 20),
    "M" -> (1L << 20),
    "GIB" -> (1L << 30),
    "GB" -> (1L << 30),
    "G" -> (1L << 30),
    "B" -> 1L
  )

  /** Parses a human byte-size string ("16MB", "1MiB", "512K",
    * "1073741824") into a byte count. Every suffix is a binary
    * multiple: K/KB/KiB = 1024, M/MB/MiB = 1024^2, G/GB/GiB = 1024^3,
    * B or none = bytes; matching is case-insensitive and surrounding
    * whitespace is trimmed. None on a malformed or negative value.
    */
  def parseSize(s: String): Option[Long] =
    val upper = s.trim.toUpperCase(Locale.ROOT)
    if upper.isEmpty then return None
    var mult = 1L
    var digits = upper
    SizeSuffixes.find((suffix, _) => upper.endsWith(suffix)) match
      case Some((suffix, m)) =>
        mult = m
        digits = upper.substring(0, upper.length - suffix.length)
      case None => ()
    digits = digits.stripTrailing
    if digits.isEmpty || digits.exists(c => c < '0' || c > '9') then return None
    try Some(Math.multiplyExact(digits.toLong, mult))
    catch
      case _: NumberFormatException => None
      case _: ArithmeticException   => None

  /** Unit table for [[parseDuration]], matched in order so "ms" wins
    * over "m" followed by a stray "s".
    */
  private val DurationUnits: List[(String, Double)] =
    List("ns" -> 1.0, "us" -> 1e3, "ms" -> 1e6, "s" -> 1e9, "m" -> 60e9, "h" -> 3600e9)

  /** Parses the Go duration grammar — a sequence of decimal numbers
    * each followed by a unit (h, m, s, ms, us, ns), such as "30s",
    * "5m", "1h30m", "1.5s" — into nanoseconds. None on a malformed
    * string.
    */
  def parseDuration(s: String): Option[Long] =
    if s.isEmpty then return None
    var rest = s
    var total = 0.0
    while rest.nonEmpty do
      var numLen = 0
      while numLen < rest.length && (rest(numLen).isDigit || rest(numLen) == '.') do numLen += 1
      if numLen == 0 then return None
      val v =
        try rest.substring(0, numLen).toDouble
        catch case _: NumberFormatException => return None
      rest = rest.substring(numLen)
      var nanos = -1.0
      var i = 0
      while i < DurationUnits.length && nanos < 0 do
        val (unit, ns) = DurationUnits(i)
        if rest.startsWith(unit) then
          val after = rest.substring(unit.length)
          // A unit whose next character is a letter is the prefix of a
          // longer token that is not a unit at all.
          if after.isEmpty || !after(0).isLetter then
            rest = after
            nanos = ns
        i += 1
      if nanos < 0 then return None
      total += v * nanos
    if total > 9.2e18 then None else Some(total.toLong)

  /** Fixed-decimal float rendering, locale-independent. */
  def f(v: Double, decimals: Int): String = String.format(Locale.ROOT, s"%.${decimals}f", v)

  /** Renders a byte count with a binary-unit suffix: "1.0GiB",
    * "16.0MiB", "4.0KiB", "512B".
    */
  def humanBytes(n: Long): String =
    if n >= (1L << 30) then f(n / (1L << 30).toDouble, 1) + "GiB"
    else if n >= (1L << 20) then f(n / (1L << 20).toDouble, 1) + "MiB"
    else if n >= (1L << 10) then f(n / (1L << 10).toDouble, 1) + "KiB"
    else s"${n}B"

  /** Renders a possibly-negative byte delta with an explicit sign. */
  def humanBytesSigned(n: Long): String =
    if n < 0 then "-" + humanBytes(-n) else "+" + humanBytes(n)

  /** Binary MiB per second over a nanosecond window; zero when the
    * window is unmeasured.
    */
  def mbPerSec(bytes: Long, ns: Long): Double =
    if ns <= 0 then 0.0 else bytes / (1L << 20).toDouble / (ns / 1e9)

  /** Renders a throughput as "123.4MB/s" (binary MiB per second) or
    * "n/a" for an unmeasured window.
    */
  def humanRate(bytes: Long, ns: Long): String =
    if ns <= 0 then "n/a" else f(mbPerSec(bytes, ns), 1) + "MB/s"

  /** The fractional part of a nanosecond remainder (0 .. 1e9) as
    * ".ddd" with trailing zeros removed; empty for zero.
    */
  private def fraction(fracNs: Long): String =
    if fracNs == 0L then ""
    else "." + String.format(Locale.ROOT, "%09d", fracNs).reverse.dropWhile(_ == '0').reverse

  /** Renders a duration the way Go's `time.Duration` prints: zero as
    * "0s"; below one second as milliseconds ("900ms", "1.5ms");
    * otherwise "[Hh][Mm]Ss" where the hour part appears when non-zero,
    * the minute part when the hour part appears or the minutes are
    * non-zero, and the seconds carry their fraction with trailing
    * zeros removed ("5s", "5.003s", "1m0s", "1m5.25s", "1h0m0s"). The
    * caller rounds first.
    */
  def humanDuration(nanos: Long): String =
    val ns = Math.abs(nanos)
    if ns == 0L then return "0s"
    if ns < 1_000_000_000L then
      val ms = ns / 1_000_000L
      val msFrac = (ns % 1_000_000L) * 1000L // scaled to 9 digits
      return s"$ms${fraction(msFrac)}ms"
    val hours = ns / 3_600_000_000_000L
    var rem = ns % 3_600_000_000_000L
    val minutes = rem / 60_000_000_000L
    rem %= 60_000_000_000L
    val seconds = rem / 1_000_000_000L
    val frac = rem % 1_000_000_000L
    val sb = StringBuilder()
    if hours > 0 then sb.append(hours).append('h')
    if hours > 0 || minutes > 0 then sb.append(minutes).append('m')
    sb.append(seconds).append(fraction(frac)).append('s')
    sb.toString

  /** Rounds a nanosecond count to the nearest multiple of `unitNs`. */
  def roundTo(ns: Long, unitNs: Long): Long = (ns + unitNs / 2) / unitNs * unitNs
