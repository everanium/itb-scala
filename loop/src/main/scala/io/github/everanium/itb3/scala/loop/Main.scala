// Long-run stress harness. The loop utility holds one Pipeline handle
// per exercised cipher surface for minutes, hammers it with concurrent
// encrypt → decrypt → compare round-trips from N worker threads,
// rotates the outer masters and reopens the handle from its session
// blob on a schedule, and reports whether the process survived with
// every byte intact. It is the Scala binding's counterpart of the Go
// harness under tools/loop: the same flags, the same round structure,
// the same summary in both renderings.
//
// The default shape is full production: the Streaming AEAD profile
// with parallax on, wrapper on, hmac-blake3 MAC, Areion-SoEM-512 inner
// hash, 1024-bit keys, and the compile-in 512-bit nonce width, driven
// through a stream session by three workers for five minutes on 16 MiB
// plaintexts. Every worker owns a distinct CSPRNG-generated plaintext
// held for the whole run, so any cross-call state leakage inside the
// Pipeline surfaces as a data mismatch between workers rather than
// cancelling out.
//
// A failure is one of two things. A cipher, rekey or load call that
// returns a non-OK status is a worker error: the run stops, the
// summary lists it, the verdict is FAIL and the exit code 1. A
// round-trip that returns without error but with different bytes is a
// data mismatch: the process terminates on the spot with exit code 3,
// printing the worker, the iteration and the first differing offset,
// and no summary — the state that produced the wrong bytes is the
// evidence. A crash inside the shared library or the host runtime has
// no exit code of its own here; surfacing it is what the utility is
// for. Every call goes through the Scala binding's own wrapper types,
// which sit on the Java binding's JNI proxy, so a Go c-shared runtime
// and a HotSpot JVM share one process — the interaction the long run
// is meant to expose.
//
// Usage:
//
//   java -cp "$(cat loop/.classpath)" io.github.everanium.itb3.scala.loop.Main \
//          --duration 5m --goroutines 3 --shape stream --hash areion512 \
//          --mac hmac-blake3 --payload-size 16MB --memlimit auto
//
// Ctrl-C triggers a graceful shutdown: in-flight iterations complete,
// then the partial summary prints.

package io.github.everanium.itb3.scala.loop

import java.nio.charset.StandardCharsets
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.locks.{Condition, ReentrantLock, ReentrantReadWriteLock}
import java.util.concurrent.{BrokenBarrierException, CountDownLatch, CyclicBarrier, TimeUnit}

import io.github.everanium.itb3.scala.{ItbError, Opts, Pipeline, Profile}
import io.github.everanium.itb3.scala.Runtime as ItbRuntime

/** The resolved command line. */
final class Config(
    val durationNs: Long,
    val iterations: Long,
    val workersRequested: Int,
    val workers: Int,
    val shape: Shape,
    val hash: String,
    val mac: String,
    val payload: Long,
    var memlimit: Long,
    val memlimitAuto: Boolean,
    val gogc: Int,
    val parallax: Boolean,
    val wrapper: Boolean,
    val profile: String,
    val keyBits: Long,
    val nonceBits: Long,
    val blobMode: Long,
    val chunkSize: Long,
    val barrierFill: Long,
    val drbg: String,
    val gomaxprocs: Int,
    val rekeyEvery: Long,
    val blobCycleEvery: Long,
    val payloadMode: PayloadMode,
    val seed: Long,
    val jsonOutput: Boolean,
    val memprofile: String
)

/** The Pipeline handles and their retained blobs, behind the lock that
  * keeps iterations clear of handle mutation.
  */
final class Pipes:
  var stream: Option[Pipeline] = None
  var msg: Option[Pipeline] = None

  /** The blob Init handed out, replaced by every rekey; the input of
    * the next blob reopen.
    */
  var streamBlob: Array[Byte] = Array.empty
  var msgBlob: Array[Byte] = Array.empty

/** One worker's counters, read by the summary after every worker has
  * returned, and the error it stopped on.
  */
final class Counters:
  val iters = AtomicLong()
  val bytesEnc = AtomicLong()
  val bytesDec = AtomicLong()
  val nanosEnc = AtomicLong()
  val nanosDec = AtomicLong()
  private val errorLock = Object()
  private var err: Option[String] = None

  def addEncrypt(ns: Long): Unit = nanosEnc.addAndGet(ns)

  def addDecrypt(ns: Long): Unit = nanosDec.addAndGet(ns)

  def addIteration(encBytes: Long, decBytes: Long): Unit =
    iters.incrementAndGet()
    bytesEnc.addAndGet(encBytes)
    bytesDec.addAndGet(decBytes)

  /** Records the first error only. */
  def setError(text: String): Unit = errorLock.synchronized {
    if err.isEmpty then err = Some(text)
  }

  def error: Option[String] = errorLock.synchronized(err)

/** One worker's private state, owned by its thread: its plaintext, its
  * reusable pump accumulators, its generator.
  */
final class WorkerState(
    val id: Int,
    val plaintext: Array[Byte],
    val payloadMode: PayloadMode,
    val seeded: Boolean,
    val rng: Rng,
    val scratch: Array[Byte],
    val feed: Array[Byte]
):
  val wire = Acc(1 << 20)
  val plain = Acc(1 << 20)

/** The state every worker shares. */
final class RunState(
    val cfg: Config,
    val streamProfile: String,
    val msgProfile: String,
    val pipes: Pipes,
    val workers: Array[Counters],
    val warmupDone: CyclicBarrier,
    val release: CyclicBarrier,
    var active: Int
):
  /** Handle mutation. Iterations hold the read side for their whole
    * encrypt → decrypt → compare; rekey and blob reopen take the write
    * side, so no cipher call is in flight while a handle's keying
    * changes or the handle itself is swapped, and no encrypt is
    * separated from its decrypt by either.
    */
  val pipesLock = ReentrantReadWriteLock()

  val rekeys = AtomicLong()
  val blobCycles = AtomicLong()

  /** Set by the duration deadline, by a signal, or by a failing worker;
    * checked by every worker before it starts an iteration.
    */
  @volatile var stop = false

  val doneLock = ReentrantLock()
  val doneCond: Condition = doneLock.newCondition()
  var finishNanos = 0L

  var rssWarmup = 0L
  var rssPeak = 0L
  var rssFinal = 0L
  var poolWarmup: Array[Long] = Array.empty
  var poolSteady: Array[Long] = Array.empty

/** The raw flag values before validation. */
private final class RawFlags:
  var barrierFill = 0L
  var blobCycleEvery = 0L
  var blobMode = 1L
  var chunkSize = "0"
  var drbg = ""
  var duration = "5m"
  var gogc = 0L
  var gomaxprocs = 0L
  var goroutines = 3L
  var hash = "areion512"
  var iterations = 0L
  var jsonOutput = false
  var keyBits = 0L
  var mac = "hmac-blake3"
  var memlimit = "auto"
  var memprofile = ""
  var nonceBits = 0L
  var parallax = "on"
  var payloadMode = "fixed"
  var payloadSize = "16MB"
  var profile = ""
  var rekeyEvery = 0L
  var seed = 0L
  var shape = "stream"
  var wrapper = "on"

/** One command-line flag: its name, the type label the usage prints,
  * its help text, whether it takes a value, the default-value suffix
  * the usage appends, and the store that parses a value into the raw
  * flags. Values are validated after the whole line is parsed. The
  * table is in alphabetical order — the order the usage prints.
  */
private final case class Flag(
    name: String,
    typeLabel: String,
    help: String,
    isBool: Boolean,
    defaultSuffix: String,
    store: (RawFlags, String) => Boolean
)

object Main:

  /** --goroutines ceiling; the harness targets modest hosts and each
    * worker pins payload-sized buffers for the whole run.
    */
  val MaxWorkers = 10

  /** The concurrency mode this binding implements, as the summary
    * reports it (shared-handle / independent-handles / single).
    */
  val Concurrency = "shared-handle"

  /** Largest slice fed to a stream session per write; the drain after
    * every write uses the same bound.
    */
  val PumpSlice: Int = 1 << 20

  /** Profiles the shape-based pair is built against when --profile is
    * empty.
    */
  private val DefaultStreamProfile = "streaming-aead-triple-mac-v1"
  private val DefaultMessageProfile = "singlemsg-triple-mac-v1"

  /** The keystream-capable primitive supplied for a layer a profile
    * leaves unnamed: PRF-grade, so sound outside the barrier, and the
    * closest relative of the AES-based inner primitive whose profiles
    * need the fill.
    */
  private val KeystreamFillCipher = "aescmac"

  /** The parallax segment size a filled palette runs with — the
    * library's own default; a schedule rejects zero.
    */
  private val KeystreamFillSegment = 4093L

  /** Prints one prefixed status line to stdout. */
  def logLine(line: String): Unit = println("[loop] " + line)

  def onOff(b: Boolean): String = if b then "on" else "off"

  /** Renders an encoder policy env value for the summary: the raw
    * string when set, "default" when the shipped ladder applies.
    */
  def policyLabel(name: String): String =
    val v = System.getenv(name)
    if v == null || v.trim.isEmpty then "default" else v.stripLeading

  private def defaultOf(v: Long): String = if v != 0L then s" (default $v)" else ""

  private def defaultOf(v: String): String = if v.isEmpty then "" else s""" (default "$v")"""

  private def storeInt(s: String): Option[Long] = s.toLongOption

  private val Flags: List[Flag] = buildFlags()

  private def buildFlags(): List[Flag] =
    val d = RawFlags()
    List(
      Flag(
        "barrier-fill", "int",
        "DRBG barrier fill margin: 1 | 2 | 4 | 8 | 16 | 32; 0 = profile default (1)",
        false, defaultOf(d.barrierFill),
        (f, s) => storeInt(s).exists(v => { f.barrierFill = v; true })
      ),
      Flag(
        "blob-cycle-every", "int",
        "reopen each pipeline from its session blob every N iterations per worker; 0 = never",
        false, "",
        (f, s) => storeInt(s).exists(v => { f.blobCycleEvery = v; true })
      ),
      Flag(
        "blob-mode", "int",
        "container floor sizing mode: 1 (per-region, default) | 2 (per-container)",
        false, defaultOf(d.blobMode),
        (f, s) => storeInt(s).exists(v => { f.blobMode = v; true })
      ),
      Flag(
        "chunk-size", "string",
        "streaming chunk-size budget (e.g. 4MB); 0 = profile default; " +
          "inert for pure message shape",
        false, defaultOf(d.chunkSize),
        (f, s) => { f.chunkSize = s; true }
      ),
      Flag(
        "drbg", "string",
        "DRBG fill primitive name (see itb3 drbgs); empty = profile default (auto tier)",
        false, defaultOf(d.drbg),
        (f, s) => { f.drbg = s; true }
      ),
      Flag(
        "duration", "duration",
        "run duration (Go format: 30s / 5m / 1h); ignored when --iterations > 0",
        false, defaultOf(d.duration),
        (f, s) => { f.duration = s; true }
      ),
      Flag(
        "gogc", "int",
        "GC trigger percentage; 0 = leave the runtime default",
        false, defaultOf(d.gogc),
        (f, s) => storeInt(s).exists(v => { f.gogc = v; true })
      ),
      Flag(
        "gomaxprocs", "int",
        "Go runtime GOMAXPROCS override; 0 = inherit from the environment",
        false, defaultOf(d.gomaxprocs),
        (f, s) => storeInt(s).exists(v => { f.gomaxprocs = v; true })
      ),
      Flag(
        "goroutines", "int",
        "concurrent workers (1..10); on runtimes without parallelism values above 1 " +
          "are clamped to 1",
        false, defaultOf(d.goroutines),
        (f, s) => storeInt(s).exists(v => { f.goroutines = v; true })
      ),
      Flag(
        "hash", "string",
        "inner ITB hash primitive name",
        false, defaultOf(d.hash),
        (f, s) => { f.hash = s; true }
      ),
      Flag(
        "iterations", "int",
        "fixed per-worker iteration count; 0 = duration-based",
        false, "",
        (f, s) => storeInt(s).exists(v => { f.iterations = v; true })
      ),
      Flag(
        "json-output", "",
        "print the final summary as one compact JSON object instead of log lines",
        true, "",
        (f, s) =>
          s match
            case "true"  => f.jsonOutput = true; true
            case "false" => f.jsonOutput = false; true
            case _       => false
      ),
      Flag(
        "key-bits", "int",
        "per-seed key width in bits: 512 | 1024 | 2048; 0 = profile default (1024)",
        false, defaultOf(d.keyBits),
        (f, s) => storeInt(s).exists(v => { f.keyBits = v; true })
      ),
      Flag(
        "mac", "string",
        "MAC primitive name",
        false, defaultOf(d.mac),
        (f, s) => { f.mac = s; true }
      ),
      Flag(
        "memlimit", "string",
        "Go heap soft limit: auto (1GiB when goroutines <= 3, else 256MiB, applied only " +
          "when the runtime has no limit) or a size (e.g. 512MB)",
        false, defaultOf(d.memlimit),
        (f, s) => { f.memlimit = s; true }
      ),
      Flag(
        "memprofile", "string",
        "write a Go runtime heap profile (pprof) to this path at the end of the run; " +
          "empty = none",
        false, defaultOf(d.memprofile),
        (f, s) => { f.memprofile = s; true }
      ),
      Flag(
        "nonce-bits", "int",
        "on-wire nonce width in bits: 128 | 256 | 512; 0 = profile default (512)",
        false, defaultOf(d.nonceBits),
        (f, s) => storeInt(s).exists(v => { f.nonceBits = v; true })
      ),
      Flag(
        "parallax", "string",
        "parallax layer: on | off",
        false, defaultOf(d.parallax),
        (f, s) => { f.parallax = s; true }
      ),
      Flag(
        "payload-mode", "string",
        "plaintext content: fixed | rotating | pattern-zero | pattern-ff | pattern-ascii",
        false, defaultOf(d.payloadMode),
        (f, s) => { f.payloadMode = s; true }
      ),
      Flag(
        "payload-size", "string",
        "per-iteration plaintext size (e.g. 1MB / 16MB / 64MB)",
        false, defaultOf(d.payloadSize),
        (f, s) => { f.payloadSize = s; true }
      ),
      Flag(
        "profile", "string",
        "exercise this single registered triple profile (overrides --shape with the " +
          "profile's surface); empty = shape-based profile pair",
        false, defaultOf(d.profile),
        (f, s) => { f.profile = s; true }
      ),
      Flag(
        "rekey-every", "int",
        "rotate the parallax + wrapper masters via Rekey every N iterations per worker; " +
          "0 = never",
        false, "",
        (f, s) => storeInt(s).exists(v => { f.rekeyEvery = v; true })
      ),
      Flag(
        "seed", "uint",
        "deterministic plaintext RNG seed for bug reproduction, NOT for security testing " +
          "(pipeline keys stay CSPRNG-drawn); 0 = crypto/rand plaintexts",
        false, "",
        (f, s) =>
          try
            f.seed = java.lang.Long.parseUnsignedLong(s)
            true
          catch case _: NumberFormatException => false
      ),
      Flag(
        "shape", "string",
        "cipher surface to exercise: stream | message | stream_one_shot | both",
        false, defaultOf(d.shape),
        (f, s) => { f.shape = s; true }
      ),
      Flag(
        "wrapper", "string",
        "wrapper layer: on | off",
        false, defaultOf(d.wrapper),
        (f, s) => { f.wrapper = s; true }
      )
    )

  private def usage(): Unit =
    System.err.println("Usage of loop:")
    for fl <- Flags do
      System.err.println(
        if fl.typeLabel.isEmpty then s"  -${fl.name}" else s"  -${fl.name} ${fl.typeLabel}"
      )
      System.err.println("    \t" + fl.help + fl.defaultSuffix)

  /** Parses argv into the raw flag values. Accepts -name value, --name
    * value, -name=value and --name=value; a boolean flag takes no value
    * unless given as -name=true / -name=false. Some(true) for -h /
    * --help (usage printed); None after printing the error.
    */
  private def parseArgv(args: Array[String], f: RawFlags): Option[Boolean] =
    var i = 0
    while i < args.length do
      val arg = args(i)
      if arg.length <= 1 || arg(0) != '-' then
        System.err.println(s"loop: unexpected positional arguments: [$arg]")
        return None
      var name = if arg.startsWith("--") then arg.substring(2) else arg.substring(1)
      if name == "h" || name == "help" then
        usage()
        return Some(true)
      var inline: Option[String] = None
      val eq = name.indexOf('=')
      if eq >= 0 then
        inline = Some(name.substring(eq + 1))
        name = name.substring(0, eq)
      Flags.find(_.name == name) match
        case None =>
          System.err.println(s"loop: flag provided but not defined: -$name")
          usage()
          return None
        case Some(fl) =>
          val value = inline match
            case Some(v) => v
            case None =>
              if fl.isBool then "true"
              else
                i += 1
                if i >= args.length then
                  System.err.println(s"loop: flag needs an argument: -${fl.name}")
                  return None
                args(i)
          if !fl.store(f, value) then
            System.err.println(s"""loop: invalid value "$value" for flag -${fl.name}""")
            return None
      i += 1
    Some(false)

  /** Whether name is in the shipped hash registry the binding
    * returns.
    */
  private def hashRegistered(name: String): Boolean =
    Pipeline.hashNames() match
      case Right(names) => names.contains(name)
      case Left(_)      => false

  /** Resolves a registered profile to the shape family its record's
    * mode exposes by reading the record through the binding's lookup: a
    * mode beginning with "streaming" exposes the stream surfaces, one
    * beginning with "singlemsg" the message surface, "blob-only" none.
    * Prints the validation message and returns None on rejection.
    */
  private def profileSurface(name: String): Option[Shape] =
    Pipeline.lookup(name) match
      case Left(_) =>
        System.err.println(s"""loop: --profile "$name" is not a registered triple profile""")
        None
      case Right(p) =>
        if p.mode().startsWith("streaming") then Some(Shape.Stream)
        else if p.mode().startsWith("singlemsg") then Some(Shape.Message)
        else
          System.err.println(
            s"""loop: --profile "$name" carries no cipher surface (blob-only mode)"""
          )
          None

  /** Applies a --profile's surface to the requested shape: a
    * message-surface profile forces message; a stream-surface profile
    * keeps stream or stream_one_shot as requested and turns message or
    * both into stream.
    */
  private def narrowShape(requested: Shape, surface: Shape): Shape =
    if surface == Shape.Message then Shape.Message
    else if requested == Shape.StreamOneShot then Shape.StreamOneShot
    else Shape.Stream

  /** Builds the resolved config from argv. Returns (None, 0) for help
    * and (None, 2) after printing "loop: <message>" for the first
    * failing rule.
    */
  private def parseFlags(args: Array[String]): (Option[Config], Int) =
    val f = RawFlags()
    val help = parseArgv(args, f) match
      case None    => return (None, 2)
      case Some(h) => h
    if help then return (None, 0)
    val durationNs = Size.parseDuration(f.duration) match
      case Some(v) if v > 0 => v
      case _ =>
        System.err.println(s"loop: --duration must be positive, got ${f.duration}")
        return (None, 2)
    if f.iterations < 0 then
      System.err.println(s"loop: --iterations must be >= 0, got ${f.iterations}")
      return (None, 2)
    if f.goroutines < 1 || f.goroutines > MaxWorkers then
      System.err.println(s"loop: --goroutines must be in 1..$MaxWorkers, got ${f.goroutines}")
      return (None, 2)
    // Concurrency mode. This binding runs shared-handle: JVM platform
    // threads call into one Pipeline handle concurrently. The Scala
    // Pipeline is a thin lifetime wrapper holding one Java Pipeline,
    // which is itself a long over an opaque Go-side registry key; every
    // entry it is passed to is re-entrant after construction, and the
    // one piece of mutable machinery behind it — the Java layer's
    // pooled direct scratch pair — is taken with an atomic swap so a
    // concurrent caller falls back to fresh buffers instead of sharing.
    // So --goroutines is the thread count verbatim, never clamped.
    val workers = f.goroutines.toInt
    var shape = Shape.parse(f.shape) match
      case Some(s) => s
      case None =>
        System.err.println(
          s"""loop: --shape must be stream | message | stream_one_shot | both, got "${f.shape}""""
        )
        return (None, 2)
    if !hashRegistered(f.hash) then
      System.err.println(s"""loop: --hash "${f.hash}" is not a registered hash primitive""")
      return (None, 2)
    // --mac is validated by Init: the C ABI enumerates no MAC names.
    val payload = Size.parseSize(f.payloadSize) match
      case Some(v) => v
      case None =>
        System.err.println(s"""loop: --payload-size: invalid size "${f.payloadSize}"""")
        return (None, 2)
    if payload < 1 then
      System.err.println("loop: --payload-size must be at least 1 byte")
      return (None, 2)
    val memlimitAuto = f.memlimit == "auto"
    val memlimit =
      if memlimitAuto then (if workers <= 3 then 1L << 30 else 256L << 20)
      else
        Size.parseSize(f.memlimit) match
          case Some(v) => v
          case None =>
            System.err.println(s"""loop: --memlimit: invalid size "${f.memlimit}"""")
            return (None, 2)
    if f.gogc < 0 then
      System.err.println(s"loop: --gogc must be >= 0, got ${f.gogc}")
      return (None, 2)
    val parallax = f.parallax match
      case "on"  => true
      case "off" => false
      case _ =>
        System.err.println(s"""loop: --parallax must be on | off, got "${f.parallax}"""")
        return (None, 2)
    val wrapper = f.wrapper match
      case "on"  => true
      case "off" => false
      case _ =>
        System.err.println(s"""loop: --wrapper must be on | off, got "${f.wrapper}"""")
        return (None, 2)
    if f.profile.nonEmpty then
      profileSurface(f.profile) match
        case None          => return (None, 2)
        case Some(surface) => shape = narrowShape(shape, surface)
    if !Set(0L, 512L, 1024L, 2048L).contains(f.keyBits) then
      System.err.println(
        s"loop: --key-bits must be 512 | 1024 | 2048 (or 0 = profile default), got ${f.keyBits}"
      )
      return (None, 2)
    if !Set(0L, 128L, 256L, 512L).contains(f.nonceBits) then
      System.err.println(
        s"loop: --nonce-bits must be 128 | 256 | 512 (or 0 = profile default), got ${f.nonceBits}"
      )
      return (None, 2)
    if !Set(1L, 2L).contains(f.blobMode) then
      System.err.println(
        s"loop: --blob-mode must be 1 (per-region) | 2 (per-container), got ${f.blobMode}"
      )
      return (None, 2)
    if !Set(0L, 1L, 2L, 4L, 8L, 16L, 32L).contains(f.barrierFill) then
      System.err.println(
        "loop: --barrier-fill must be 1 | 2 | 4 | 8 | 16 | 32 (or 0 = profile default), " +
          s"got ${f.barrierFill}"
      )
      return (None, 2)
    val chunkSize = Size.parseSize(f.chunkSize) match
      case Some(v) => v
      case None =>
        System.err.println(s"""loop: --chunk-size: invalid size "${f.chunkSize}"""")
        return (None, 2)
    if f.gomaxprocs < 0 then
      System.err.println(s"loop: --gomaxprocs must be > 0 when specified, got ${f.gomaxprocs}")
      return (None, 2)
    if f.rekeyEvery < 0 then
      System.err.println(s"loop: --rekey-every must be >= 0, got ${f.rekeyEvery}")
      return (None, 2)
    if f.blobCycleEvery < 0 then
      System.err.println(s"loop: --blob-cycle-every must be >= 0, got ${f.blobCycleEvery}")
      return (None, 2)
    val payloadMode = PayloadMode.parse(f.payloadMode) match
      case Some(m) => m
      case None =>
        System.err.println(
          "loop: --payload-mode must be fixed | rotating | pattern-zero | pattern-ff | " +
            s"""pattern-ascii, got "${f.payloadMode}""""
        )
        return (None, 2)
    (
      Some(
        Config(
          durationNs, f.iterations, workers, workers, shape, f.hash, f.mac, payload,
          memlimit, memlimitAuto, f.gogc.toInt, parallax, wrapper, f.profile, f.keyBits,
          f.nonceBits, f.blobMode, chunkSize, f.barrierFill,
          f.drbg, // validated by Init: the C ABI enumerates no DRBG names
          f.gomaxprocs.toInt, f.rekeyEvery,
          f.blobCycleEvery, payloadMode, f.seed, f.jsonOutput, f.memprofile
        )
      ),
      0
    )

  @volatile private var signalSeen = false

  /** Counted down once the summary has been emitted, so the shutdown
    * hook can hold the JVM's own termination back until then.
    */
  private val summaryDone = CountDownLatch(1)

  /** Graceful stop. SIGINT / SIGTERM set a flag the main thread polls
    * while it waits for the workers; it turns the flag into the stop
    * request every worker checks before starting an iteration, so a
    * signal interrupts nothing mid-call — the in-flight encrypt /
    * decrypt / compare completes, the worker returns, and the partial
    * summary prints with the verdict the completed iterations earned.
    * Scala-specific: the supported way to observe a termination signal
    * on this runtime is a shutdown hook, which runs concurrently with
    * the main thread rather than in place of it and cannot set the exit
    * code, so the hook only raises the flag and then blocks until the
    * summary is out; the verdict's code is then delivered by halting
    * the runtime from the main thread, which is also what makes the
    * exit code the verdict's rather than the JVM's own signal code.
    */
  private def installSignals(): Unit =
    val body: Runnable = () =>
      signalSeen = true
      try summaryDone.await(2, TimeUnit.MINUTES)
      catch case _: InterruptedException => Thread.currentThread().interrupt()
      ()
    val hook = Thread(body, "loop-signal")
    java.lang.Runtime.getRuntime.addShutdownHook(hook)

  /** Supplies the keystream-capable primitive for every layer the
    * profile record leaves unnamed and the run engages: a missing
    * parallax palette becomes three copies of the fill cipher (with the
    * library's default segment size when the record carries none), a
    * missing outer cipher becomes the fill cipher. These are opts
    * overrides that fold into the resolved record the blob carries — a
    * derived profile is never registered, so no name the receiver did
    * not agree to reaches the wire. None after printing the validation
    * message.
    */
  private def fillKeystreamLayers(
      name: String,
      opts: Opts,
      wantParallax: Boolean,
      wantWrapper: Boolean
  ): Option[(Opts, Boolean)] =
    Pipeline.lookup(name) match
      case Left(_) =>
        System.err.println(s"""loop: --profile "$name" is not a registered triple profile""")
        None
      case Right(p) =>
        var filled = false
        var out = opts
        if wantParallax && p.palette().isEmpty then
          out = out.withParallaxPalette(
            KeystreamFillCipher,
            KeystreamFillCipher,
            KeystreamFillCipher
          )
          // A recipe that never carried a palette never carried a
          // segment size either, and the schedule rejects zero.
          if p.segment() == 0 then out = out.withParallaxSegmentSize(KeystreamFillSegment)
          filled = true
        if wantWrapper && p.outer().isEmpty then
          out = out.withOuterCipher(KeystreamFillCipher)
          filled = true
        Some((out, filled))

  /** Constructs one Pipeline against profile with every flag-carried
    * override in the opts string (zero values included — the shared
    * library treats zero as "profile default"), then obtains the Init
    * blob once through save: the binding's init entry does not hand the
    * blob back, and the bytes are the ones Init produced. Later blob
    * reopens use the retained blob; save is never called again.
    */
  private def buildPipeline(cfg: Config, profile: String): Option[(Pipeline, Array[Byte])] =
    var opts = Opts.empty
      .withInnerHash(cfg.hash)
      .withMacName(cfg.mac)
      .withParallax(cfg.parallax)
      .withWrapper(cfg.wrapper)
      .withKeyBits(cfg.keyBits)
      .withNonceBits(cfg.nonceBits)
      .withBarrierFill(cfg.barrierFill)
      .withDrbg(cfg.drbg)
      .withChunkSize(cfg.chunkSize)
    if cfg.profile.nonEmpty then
      fillKeystreamLayers(cfg.profile, opts, cfg.parallax, cfg.wrapper) match
        case None => return None
        case Some((o, filled)) =>
          opts = o
          if filled then
            System.err.println(
              s"loop: ${cfg.profile} leaves the requested keystream layers unnamed; " +
                s"$KeystreamFillCipher supplied for them"
            )
    var pipe = Pipeline.init(profile, opts) match
      case Left(e) =>
        System.err.println(s"loop: Init($profile): ${Worker.detail(e)}")
        return None
      case Right(p) => p
    val blob = pipe.save() match
      case Left(e) =>
        System.err.println(s"loop: Save($profile): ${Worker.detail(e)}")
        pipe.close()
        return None
      case Right(b) => b
    if cfg.blobMode == 2L then
      // The sizing mode is not an Opts knob: the Init blob is edited
      // and the pipeline reopened from it, so the retained blob (the
      // one blob-cycle reopens from) carries the edited mode.
      if !editInnerBlobMode(blob, 2) then
        System.err.println("loop: rewrite blob mode: inner blob mode field not found")
        pipe.close()
        return None
      pipe.close()
      pipe = Pipeline.load(blob) match
        case Left(e) =>
          System.err.println(s"loop: reload Mode 2 blob: ${Worker.detail(e)}")
          return None
        case Right(p) => p
    logPipelineInitialised(profile, blob)
    Some((pipe, blob))

  /** Offset of the first occurrence of needle in hay at or after from,
    * or -1 when absent.
    */
  private def findBytes(hay: Array[Byte], from: Int, needle: Array[Byte]): Int =
    var i = from
    while i + needle.length <= hay.length do
      var j = 0
      while j < needle.length && hay(i + j) == needle(j) do j += 1
      if j == needle.length then return i
      i += 1
    -1

  /** Sets the inner blob's "mode" field of a wrap-layer session blob to
    * targetMode (1 = per-region, 2 = per-container) in place. The wrap
    * layer's profile record carries its own "mode" (a string), so the
    * search starts at the inner blob ("ib"); both shipped modes are one
    * digit wide, so the blob length does not change and the key material
    * in blob is never copied. Returns false when the inner blob or its
    * mode field is not found.
    */
  private def editInnerBlobMode(blob: Array[Byte], targetMode: Int): Boolean =
    val ibKey = "\"ib\":{".getBytes(StandardCharsets.US_ASCII)
    val modeKey = "\"mode\":".getBytes(StandardCharsets.US_ASCII)
    val ib = findBytes(blob, 0, ibKey)
    if ib < 0 then return false
    val mode = findBytes(blob, ib + ibKey.length, modeKey)
    if mode < 0 then return false
    val at = mode + modeKey.length
    if at + 1 >= blob.length || blob(at) < '1' || blob(at) > '2' ||
        (blob(at + 1) >= '0' && blob(at + 1) <= '9')
    then return false
    blob(at) = ('0' + targetMode).toByte
    true

  private def dash(s: String): String = if s.isEmpty then "-" else s

  /** Prints the construction line with the recipe read back from the
    * blob the Pipeline handed out, not echoed from the flags: every
    * construction override is proven to have reached the library by the
    * value the receiver would see. Record values that are empty (a No
    * MAC profile's MAC, a mixed profile's single hash) print as "-".
    */
  private def logPipelineInitialised(profile: String, blob: Array[Byte]): Unit =
    Pipeline.inspect(blob) match
      case Left(e) =>
        logLine(
          s"pipeline initialised: profile=$profile blob=${blob.length} bytes " +
            s"(inspect: ${Worker.detail(e)})"
        )
      case Right(rec) =>
        logLine(
          s"pipeline initialised: profile=$profile blob=${blob.length} bytes" +
            s" hash=${dash(rec.hash())}" +
            s" key-bits=${rec.keyBits()}" +
            s" nonce-bits=${Option(rec.nonceBits()).map(_.intValue).getOrElse(0)}" +
            s" barrier-fill=${Option(rec.barrierFill()).map(_.intValue).getOrElse(0)}" +
            s" chunk-size=${rec.chunk()}" +
            s" mac=${dash(rec.mac())}" +
            s" parallax=${onOff(rec.parallax())}" +
            s" wrapper=${onOff(rec.wrapper())}" +
            (if rec.containerMode() != null && rec.containerMode().intValue == 2 then
               " container-mode=2"
             else "") +
            (if rec.drbg().isEmpty then "" else s" drbg=${rec.drbg()}")
        )

  private def run(args: Array[String]): Int =
    val cfg = parseFlags(args) match
      case (None, code) => return code
      case (Some(c), _) => c

    // Runtime shaping. A long run under allocation churn grows the Go
    // heap inside the shared library without bound unless a soft limit
    // paces the collector, so a limit is always in force: an explicit
    // --memlimit is set as given, and auto caps the heap only when the
    // runtime reports no limit at all (a limit already installed from
    // the environment is left standing). The GC percentage and
    // GOMAXPROCS are set only when their flag is non-zero — a zero flag
    // skips the setter rather than calling it with zero, because zero
    // is a real value to the GC-percent setter, and a call would
    // clobber whatever the environment installed. All of it lands
    // before any Pipeline exists so the baselines are taken under the
    // shaped runtime.
    if cfg.memlimitAuto then
      if ItbRuntime.setMemoryLimit(-1) == Long.MaxValue then
        ItbRuntime.setMemoryLimit(cfg.memlimit)
    else ItbRuntime.setMemoryLimit(cfg.memlimit)
    cfg.memlimit = ItbRuntime.setMemoryLimit(-1)
    if cfg.gogc > 0 then ItbRuntime.setGCPercent(cfg.gogc)
    if cfg.gomaxprocs > 0 then ItbRuntime.setGOMAXPROCS(cfg.gomaxprocs)

    logLine(
      s"start: duration=${Size.humanDuration(cfg.durationNs)}" +
        s" iterations=${cfg.iterations}" +
        s" goroutines=${cfg.workersRequested}" +
        s" workers=${cfg.workers}" +
        s" concurrency=$Concurrency" +
        s" shape=${cfg.shape.label}" +
        s" hash=${cfg.hash}" +
        s" mac=${cfg.mac}" +
        s" payload=${Size.humanBytes(cfg.payload)}" +
        s" memlimit=${Size.humanBytes(cfg.memlimit)}" +
        s" parallax=${onOff(cfg.parallax)}" +
        s" wrapper=${onOff(cfg.wrapper)}"
    )
    logLine(
      s"""overrides: profile="${cfg.profile}"""" +
        s" key-bits=${cfg.keyBits}" +
        s" nonce-bits=${cfg.nonceBits}" +
        s" chunk-size=${Size.humanBytes(cfg.chunkSize)}" +
        s" barrier-fill=${cfg.barrierFill}" +
        s" gomaxprocs=${cfg.gomaxprocs}" +
        s" rekey-every=${cfg.rekeyEvery}" +
        s" blob-cycle-every=${cfg.blobCycleEvery}" +
        s" payload-mode=${cfg.payloadMode.label}" +
        s" seed=${java.lang.Long.toUnsignedString(cfg.seed)}" +
        s" json-output=${if cfg.jsonOutput then "true" else "false"}" +
        (if cfg.blobMode != 1L then s" blob-mode=${cfg.blobMode}" else "") +
        (if cfg.drbg.isEmpty then "" else s" drbg=${cfg.drbg}")
    )
    logLine(
      s"""policy: microbatch-tiers=${policyLabel("ITB_MICROBATCH_TIERS")}""" +
        s""" hashpool-starters=${policyLabel("ITB_HASHPOOL_STARTERS")}"""
    )

    // Pipeline construction — one shared handle per exercised shape.
    // stream and stream_one_shot share the streaming handle.
    val streamProfile = if cfg.profile.isEmpty then DefaultStreamProfile else cfg.profile
    val msgProfile = if cfg.profile.isEmpty then DefaultMessageProfile else cfg.profile
    val pipes = Pipes()
    if cfg.shape == Shape.Stream || cfg.shape == Shape.StreamOneShot || cfg.shape == Shape.Both
    then
      buildPipeline(cfg, streamProfile) match
        case None => return 1
        case Some((p, b)) =>
          pipes.stream = Some(p)
          pipes.streamBlob = b
    if cfg.shape == Shape.Message || cfg.shape == Shape.Both then
      buildPipeline(cfg, msgProfile) match
        case None => return 1
        case Some((p, b)) =>
          pipes.msg = Some(p)
          pipes.msgBlob = b

    // Allocation posture. Per-worker plaintexts are allocated once and
    // held for the whole run (rotating mode refills them in place per
    // iteration); the pump accumulators, the drain scratch and the feed
    // buffer live inside each worker and are reused across iterations;
    // the message and one-shot outputs are allocated by the binding per
    // call and reclaimed per iteration. Under the default fixed CSPRNG
    // mode every worker's buffer is distinct, so cross-worker data
    // crossover is detectable; pattern modes trade that property for
    // content edge-case coverage.
    if cfg.payload > Int.MaxValue.toLong - 8 then
      System.err.println("loop: --payload-size exceeds the largest JVM array")
      return 1
    val built = scala.collection.mutable.ListBuffer[WorkerState]()
    var fillOk = true
    var id = 0
    while id < cfg.workers && fillOk do
      val w = WorkerState(
        id,
        new Array[Byte](cfg.payload.toInt),
        cfg.payloadMode,
        cfg.seed != 0L,
        Rng(Payload.seedWorker(cfg.seed, id)),
        new Array[Byte](PumpSlice),
        new Array[Byte](PumpSlice)
      )
      if Payload.fill(cfg.payloadMode, w.seeded, w.rng, w.plaintext) then built += w
      else fillOk = false
      id += 1
    if !fillOk then
      System.err.println("loop: payload fill: csprng")
      return 1
    val states = built.toList

    installSignals()
    val r = RunState(
      cfg,
      streamProfile,
      msgProfile,
      pipes,
      Array.fill(cfg.workers)(Counters()),
      CyclicBarrier(cfg.workers + 1),
      CyclicBarrier(cfg.workers + 1),
      cfg.workers
    )

    // Warmup barrier. Every worker runs one iteration and waits; the
    // clock starts only once all of them have paid their first-call
    // costs (pool warm-up, lazy kernel dispatch, page faults on the
    // payload buffers, and on this runtime the tiered JIT's first pass
    // over the iteration body), and the RSS and pool baselines taken
    // here describe a process that has already run the whole cipher
    // path once per worker.
    val warmupStart = System.nanoTime()
    val threads = states.map { w =>
      val t = Thread(() => Worker.run(r, w), s"loop-worker-${w.id}")
      t.setDaemon(false)
      t.start()
      t
    }
    try r.warmupDone.await()
    catch
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        System.err.println(s"loop: warmup barrier: $e")
        return 1
      case e: BrokenBarrierException =>
        System.err.println(s"loop: warmup barrier: $e")
        return 1
    val (rssWarmup, _) = Summary.readRss()
    val poolWarmup = Summary.poolSnapshot()
    logLine(
      s"warmup: ${cfg.workers} workers x 1 iter completed in " +
        Size.humanDuration(Size.roundTo(System.nanoTime() - warmupStart, 100_000_000L)) +
        s" (baseline rss=${Size.humanBytes(rssWarmup)})"
    )

    // Open the gate; the duration is a deadline the waiter below
    // enforces in duration mode.
    val start = System.nanoTime()
    try r.release.await()
    catch
      case e: InterruptedException =>
        Thread.currentThread().interrupt()
        System.err.println(s"loop: release barrier: $e")
        return 1
      case e: BrokenBarrierException =>
        System.err.println(s"loop: release barrier: $e")
        return 1

    // Wait for every worker, polling every 100 ms so the deadline and a
    // signal are both noticed promptly.
    var finish = start
    r.doneLock.lock()
    try
      while r.active > 0 do
        if signalSeen || (cfg.iterations == 0 && System.nanoTime() - start >= cfg.durationNs)
        then r.stop = true
        try r.doneCond.await(100, TimeUnit.MILLISECONDS)
        catch
          case _: InterruptedException =>
            Thread.currentThread().interrupt()
            r.stop = true
      if r.finishNanos != 0L then finish = r.finishNanos
    finally r.doneLock.unlock()
    val elapsedNs = finish - start
    val (rssFinal, rssPeak) = Summary.readRss()
    val poolSteady = Summary.poolSnapshot()
    for t <- threads do
      try t.join()
      catch case _: InterruptedException => Thread.currentThread().interrupt()
    r.rssWarmup = rssWarmup
    r.rssFinal = rssFinal
    r.rssPeak = rssPeak
    r.poolWarmup = poolWarmup
    r.poolSteady = poolSteady

    if cfg.memprofile.nonEmpty then
      ItbRuntime.writeHeapProfile(cfg.memprofile) match
        case Right(_) => logLine(s"memprofile: heap profile written to ${cfg.memprofile}")
        case Left(e)  => System.err.println(s"loop: memprofile: ${Worker.detail(e)}")

    val exit = Summary.emit(r, elapsedNs)
    r.pipes.stream.foreach(_.close())
    r.pipes.msg.foreach(_.close())
    exit

  /** Restores the default disposition of SIGPIPE.
    *
    * Scala-specific. The runtime ignores the signal and the standard
    * streams swallow the write error that replaces it, so a consumer
    * that stops reading leaves the process printing into nothing and
    * exiting 0 with its verdict undelivered. With the default
    * disposition back the first such write ends the process, which is
    * what every other implementation does and what a fleet driver
    * expects.
    */
  def restoreSigpipe(): Unit =
    sun.misc.Signal.handle(new sun.misc.Signal("PIPE"), sun.misc.SignalHandler.SIG_DFL)

  /** Entry point. */
  def main(args: Array[String]): Unit =
    restoreSigpipe()
    val code = run(args)
    System.out.flush()
    System.err.flush()
    summaryDone.countDown()
    // Scala-specific. The verdict's code is delivered by halting the
    // runtime rather than by returning from main: when a signal has
    // started the shutdown sequence, returning would let the JVM finish
    // that sequence with its own signal-derived code instead of this
    // one.
    java.lang.Runtime.getRuntime.halt(code)
