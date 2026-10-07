// The final summary in both renderings, and the two measurements it
// folds in that are not per-worker counters: the process resident set
// and the shared library's pool counters.

package io.github.everanium.itb3.scala.loop

import java.util.Locale
import scala.io.Source
import scala.util.Using

import io.github.everanium.itb3.scala.Runtime as ItbRuntime

object Summary:

  /** Parses one "Vm...:   1234 kB" line of /proc/self/status into
    * bytes; zero on any parse failure.
    */
  private def statusKb(line: String): Long =
    val colon = line.indexOf(':')
    if colon < 0 then return 0L
    val parts = line.substring(colon + 1).trim.split("\\s+")
    if parts.isEmpty || parts(0).isEmpty then return 0L
    try parts(0).toLong * 1024L
    catch case _: NumberFormatException => 0L

  /** The process's current resident set and its high-water mark in
    * bytes, from /proc/self/status (VmRSS and VmHWM, reported in kB).
    * Both are zero on a platform without that file; the figures are
    * informational and never enter the verdict.
    */
  def readRss(): (Long, Long) =
    val lines =
      try Using.resource(Source.fromFile("/proc/self/status"))(_.getLines().toList)
      catch case _: Exception => return (0L, 0L)
    var current = 0L
    var peak = 0L
    for line <- lines do
      if line.startsWith("VmRSS:") then current = statusKb(line)
      else if line.startsWith("VmHWM:") then peak = statusKb(line)
    (current, peak)

  /** Pool counters. The shared library keeps process-wide monotonic
    * totals at every pool checkout of its cipher core: per hash-array
    * tier the starter width, checkouts, constructor misses, regrow
    * replacements and bytes allocated; for the scratch byte pool and
    * the parallax chunk pool the checkouts, constructor misses, regrows
    * and regrow bytes. Two snapshots bracketing the main loop are
    * differenced into per-run hit / miss figures that tell whether a
    * pool keeps its items warm between calls or evicts them across GC
    * cycles. The slot layout is read from the library: slot 0 carries
    * the tier count T, tier i occupies the five slots at 1 + 5*i, and
    * the two byte pools occupy the eight slots at 1 + 5*T; the vector
    * is sized by the binding from the library's own length query, never
    * from a constant. Empty when the library is unavailable.
    */
  def poolSnapshot(): Array[Long] =
    ItbRuntime.poolStats() match
      case Right(v) => v
      case Left(_)  => Array.empty

  /** One starter tier of the hash-array pool, differenced. */
  private final case class Tier(
      index: Long,
      starter: Long,
      get: Long,
      fresh: Long,
      regrow: Long,
      newBytes: Long
  )

  /** One single-size byte pool, differenced. */
  private final case class BytePool(
      get: Long = 0,
      fresh: Long = 0,
      regrow: Long = 0,
      regrowBytes: Long = 0
  )

  private final case class PoolDelta(
      tiers: List[Tier] = Nil,
      buf: BytePool = BytePool(),
      chunk: BytePool = BytePool()
  )

  private def poolDiff(steady: Array[Long], warmup: Array[Long]): PoolDelta =
    if steady.length < 9 || warmup.length != steady.length then return PoolDelta()
    val tiers = steady(0)
    if tiers < 0 || 1 + 5 * tiers + 8 > steady.length then return PoolDelta()
    val list = scala.collection.mutable.ListBuffer[Tier]()
    var i = 0L
    while i < tiers do
      val b = (1 + 5 * i).toInt
      if steady(b) != 0L then
        list += Tier(
          index = i,
          starter = steady(b),
          get = steady(b + 1) - warmup(b + 1),
          fresh = steady(b + 2) - warmup(b + 2),
          regrow = steady(b + 3) - warmup(b + 3),
          newBytes = steady(b + 4) - warmup(b + 4)
        )
      i += 1
    val t = (1 + 5 * tiers).toInt
    PoolDelta(
      tiers = list.toList,
      buf = BytePool(
        steady(t) - warmup(t),
        steady(t + 1) - warmup(t + 1),
        steady(t + 2) - warmup(t + 2),
        steady(t + 3) - warmup(t + 3)
      ),
      chunk = BytePool(
        steady(t + 4) - warmup(t + 4),
        steady(t + 5) - warmup(t + 5),
        steady(t + 6) - warmup(t + 6),
        steady(t + 7) - warmup(t + 7)
      )
    )

  /** Misses over checkouts as a percentage; zero when nothing was
    * checked out.
    */
  private def missPercent(miss: Long, get: Long): Double =
    if get <= 0 then 0.0 else 100.0 * miss / get

  /** Renders s as a JSON string literal with the escapes JSON
    * requires.
    */
  private def jsonString(s: String): String =
    val sb = StringBuilder()
    sb.append('"')
    for ch <- s do
      ch match
        case '"'                 => sb.append("\\\"")
        case '\\'                => sb.append("\\\\")
        case '\n'                => sb.append("\\n")
        case '\r'                => sb.append("\\r")
        case '\t'                => sb.append("\\t")
        case c if c < ' '        => sb.append(String.format(Locale.ROOT, "\\u%04x", c.toInt))
        case c                   => sb.append(c)
    sb.append('"')
    sb.toString

  /** The effective GC percentage as the runtime reports it: the query
    * form of the setter (a set-and-restore round trip inside the
    * library) so the field is the same whether the value came from the
    * flag, the environment, or the runtime default.
    */
  private def effectiveGogc(flag: Int): Int =
    if flag > 0 then flag else ItbRuntime.setGCPercent(-1)

  /** Output contract. Both renderings are shared with the Go harness
    * and every other binding's loop utility field for field: the same
    * lines in the same order, the same keys in the same order, floats
    * with a fixed number of decimals so the JSON is byte-identical
    * across implementations. The Go harness alone adds its
    * runtime-internal lines after rss: and its runtime-internal keys
    * after parallax_chunk_pool; nothing here reproduces them because
    * nothing they read is reachable through the C ABI. Returns the exit
    * code.
    */
  def emit(r: RunState, elapsedNs: Long): Int =
    val cfg = r.cfg
    val workers = cfg.workers.toLong
    var totalIters = 0L
    var totalEnc = 0L
    var totalDec = 0L
    var nanosEnc = 0L
    var nanosDec = 0L
    val perWorker = scala.collection.mutable.ListBuffer[Long]()
    val errors = scala.collection.mutable.ListBuffer[String]()
    for c <- r.workers do
      val n = c.iters.get()
      perWorker += n
      totalIters += n
      totalEnc += c.bytesEnc.get()
      totalDec += c.bytesDec.get()
      nanosEnc += c.nanosEnc.get()
      nanosDec += c.nanosDec.get()
      c.error.foreach(errors += _)

    // Throughput. Per-direction throughput divides the sum of every
    // worker's wall time in that direction by the worker count — the
    // equivalent single-stream wall time under N-way concurrency — so
    // each direction reports the aggregate rate it sustained rather
    // than collapsing to combined/2 (every iteration moves equal
    // encrypt and decrypt bytes, so a total-elapsed denominator would
    // give both directions the same figure). The combined rate keeps
    // total elapsed as the one-glance overall figure.
    val avgEnc = if nanosEnc > 0 then nanosEnc / workers else 0L
    val avgDec = if nanosDec > 0 then nanosDec / workers else 0L

    val rssDelta = r.rssFinal - r.rssWarmup
    val rssGrowth = if r.rssWarmup > 0 then 100.0 * rssDelta / r.rssWarmup else 0.0

    val pd = poolDiff(r.poolSteady, r.poolWarmup)
    val pass = errors.isEmpty
    val rekeys = r.rekeys.get()
    val cycles = r.blobCycles.get()
    val gomaxprocs = ItbRuntime.setGOMAXPROCS(0)
    val streamProfile = if r.pipes.stream.isDefined then r.streamProfile else ""
    val msgProfile = if r.pipes.msg.isDefined then r.msgProfile else ""

    if cfg.jsonOutput then
      val j = StringBuilder()
      j.append("{\"duration_seconds\":").append(Size.f(elapsedNs / 1e9, 3))
      j.append(",\"iterations\":").append(totalIters)
      j.append(",\"per_worker_iterations\":[").append(perWorker.mkString(",")).append(']')
      j.append(",\"bytes_encrypted\":").append(totalEnc)
      j.append(",\"bytes_decrypted\":").append(totalDec)
      j.append(",\"encrypt_mb_per_sec\":").append(Size.f(Size.mbPerSec(totalEnc, avgEnc), 1))
      j.append(",\"decrypt_mb_per_sec\":").append(Size.f(Size.mbPerSec(totalDec, avgDec), 1))
      j.append(",\"combined_mb_per_sec\":")
        .append(Size.f(Size.mbPerSec(totalEnc + totalDec, elapsedNs), 1))
      j.append(",\"rekeys\":").append(rekeys)
      j.append(",\"blob_cycles\":").append(cycles)
      j.append(",\"worker_errors\":[").append(errors.map(jsonString).mkString(",")).append(']')
      j.append(",\"verdict\":\"").append(if pass then "PASS" else "FAIL").append('"')
      j.append(",\"shape\":\"").append(cfg.shape.label).append('"')
      j.append(",\"stream_profile\":").append(jsonString(streamProfile))
      j.append(",\"message_profile\":").append(jsonString(msgProfile))
      j.append(",\"hash\":").append(jsonString(cfg.hash))
      j.append(",\"mac\":").append(jsonString(cfg.mac))
      j.append(",\"payload_bytes\":").append(cfg.payload)
      j.append(",\"payload_mode\":\"").append(cfg.payloadMode.label).append('"')
      j.append(",\"seed\":").append(java.lang.Long.toUnsignedString(cfg.seed))
      j.append(",\"key_bits\":").append(cfg.keyBits)
      j.append(",\"nonce_bits\":").append(cfg.nonceBits)
      j.append(",\"blob_mode\":").append(cfg.blobMode)
      j.append(",\"drbg\":").append(jsonString(cfg.drbg))
      j.append(",\"drbg_auto_tier\":").append(jsonString(ItbRuntime.drbgAutoTier))
      j.append(",\"chunk_size_bytes\":").append(cfg.chunkSize)
      j.append(",\"barrier_fill\":").append(cfg.barrierFill)
      j.append(",\"parallax\":\"").append(Main.onOff(cfg.parallax)).append('"')
      j.append(",\"wrapper\":\"").append(Main.onOff(cfg.wrapper)).append('"')
      j.append(",\"goroutines_requested\":").append(cfg.workersRequested)
      j.append(",\"goroutines\":").append(cfg.workers)
      j.append(",\"concurrency\":\"").append(Main.Concurrency).append('"')
      j.append(",\"gogc\":\"").append(effectiveGogc(cfg.gogc)).append('"')
      j.append(",\"memlimit_bytes\":").append(cfg.memlimit)
      j.append(",\"gomaxprocs\":").append(gomaxprocs)
      j.append(",\"microbatch_tiers\":").append(jsonString(Main.policyLabel("ITB_MICROBATCH_TIERS")))
      j.append(",\"hashpool_starters\":")
        .append(jsonString(Main.policyLabel("ITB_HASHPOOL_STARTERS")))
      j.append(",\"rss_warmup_bytes\":").append(r.rssWarmup)
      j.append(",\"rss_peak_bytes\":").append(r.rssPeak)
      j.append(",\"rss_final_bytes\":").append(r.rssFinal)
      j.append(",\"rss_growth_percent\":").append(Size.f(rssGrowth, 2))
      j.append(",\"hash_pool_tiers\":[")
      j.append(
        pd.tiers
          .map(t =>
            s"{\"tier\":${t.index},\"starter\":${t.starter},\"get\":${t.get}," +
              s"\"new\":${t.fresh},\"regrow\":${t.regrow},\"new_bytes\":${t.newBytes}," +
              s"\"miss_percent\":${Size.f(missPercent(t.fresh + t.regrow, t.get), 2)}}"
          )
          .mkString(",")
      )
      j.append(']')
      j.append(",\"buf_pool\":{\"get\":").append(pd.buf.get)
        .append(",\"new\":").append(pd.buf.fresh)
        .append(",\"regrow\":").append(pd.buf.regrow)
        .append(",\"regrow_bytes\":").append(pd.buf.regrowBytes)
        .append(",\"miss_percent\":").append(Size.f(missPercent(pd.buf.regrow, pd.buf.get), 2))
        .append('}')
      j.append(",\"parallax_chunk_pool\":{\"get\":").append(pd.chunk.get)
        .append(",\"new\":").append(pd.chunk.fresh)
        .append(",\"regrow\":").append(pd.chunk.regrow)
        .append(",\"regrow_bytes\":").append(pd.chunk.regrowBytes)
        .append(",\"miss_percent\":").append(Size.f(missPercent(pd.chunk.regrow, pd.chunk.get), 2))
        .append('}')
      j.append('}')
      println(j.toString)
      return if pass then 0 else 1

    Main.logLine("=== FINAL ===")
    Main.logLine("  duration: " + Size.humanDuration(Size.roundTo(elapsedNs, 1_000_000L)))
    Main.logLine("  iterations: " + perWorker.mkString(" + ") + " = " + totalIters + " total")
    Main.logLine(
      "  throughput: encrypt " + Size.humanRate(totalEnc, avgEnc) +
        ", decrypt " + Size.humanRate(totalDec, avgDec) +
        ", combined " + Size.humanRate(totalEnc + totalDec, elapsedNs)
    )
    Main.logLine(
      "  bytes: " + Size.humanBytes(totalEnc) + " encrypted, " +
        Size.humanBytes(totalDec) + " decrypted"
    )
    Main.logLine("  data integrity: " + totalIters + "/" + totalIters + " PASS")
    Main.logLine(
      "  concurrency: " + Main.Concurrency + ", workers " + cfg.workers +
        " (requested " + cfg.workersRequested + ")"
    )
    Main.logLine(
      "  rss: warmup " + Size.humanBytes(r.rssWarmup) +
        ", peak " + Size.humanBytes(r.rssPeak) +
        ", final " + Size.humanBytes(r.rssFinal) +
        " (delta " + Size.humanBytesSigned(rssDelta) +
        ", " + Size.f(rssGrowth, 1) + "% growth)"
    )
    for t <- pd.tiers do
      Main.logLine(
        "  hash pool tier " + t.index + " (starter " + t.starter + "): get " + t.get +
          ", miss " + (t.fresh + t.regrow) +
          " (new " + t.fresh + " + regrow " + t.regrow + ")" +
          ", miss " + Size.f(missPercent(t.fresh + t.regrow, t.get), 2) + "%" +
          ", " + Size.humanBytes(t.newBytes) + " allocated"
      )
    Main.logLine(
      "  buf pool: get " + pd.buf.get + ", regrow " + pd.buf.regrow +
        " (of which fresh " + pd.buf.fresh + ")" +
        ", miss " + Size.f(missPercent(pd.buf.regrow, pd.buf.get), 2) + "%" +
        ", " + Size.humanBytes(pd.buf.regrowBytes) + " regrown"
    )
    Main.logLine(
      "  parallax chunk pool: get " + pd.chunk.get + ", regrow " + pd.chunk.regrow +
        " (of which fresh " + pd.chunk.fresh + ")" +
        ", miss " + Size.f(missPercent(pd.chunk.regrow, pd.chunk.get), 2) + "%" +
        ", " + Size.humanBytes(pd.chunk.regrowBytes) + " regrown"
    )
    if rekeys > 0 then Main.logLine("  rekeys: " + rekeys)
    if cycles > 0 then Main.logLine("  blob cycles: " + cycles)
    for e <- errors do Main.logLine("  ERROR: " + e)
    if pass then
      Main.logLine("  verdict: PASS")
      0
    else
      Main.logLine("  verdict: FAIL (errors=" + errors.size + ")")
      1
