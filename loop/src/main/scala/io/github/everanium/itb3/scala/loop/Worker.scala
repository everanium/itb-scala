// The worker: its thread body (one warmup iteration, the warmup
// barrier, the main loop), one iteration, the session pump loop the
// stream shape drives, and the round-trip comparison that decides
// between a worker error and a data mismatch.

package io.github.everanium.itb3.scala.loop

import java.io.ByteArrayOutputStream
import java.util.Arrays
import java.util.concurrent.BrokenBarrierException
import scala.util.matching.Regex

import io.github.everanium.itb3.scala.{DecryptStream, EncryptStream, ItbError, Pipeline}

/** Cipher surfaces the --shape flag selects. */
enum Shape(val label: String):
  /** Session pump: begin / write / read / end. */
  case Stream extends Shape("stream")

  /** Single Message: one whole-buffer call. */
  case Message extends Shape("message")

  /** Stream surface, one whole-buffer call. */
  case StreamOneShot extends Shape("stream_one_shot")

  /** All three, rotating by iteration number. */
  case Both extends Shape("both")

object Shape:
  def parse(s: String): Option[Shape] = Shape.values.find(_.label == s)

/** A growable output accumulator whose backing array the comparison
  * reads without a copy.
  *
  * Scala-specific. `ByteArrayOutputStream.toByteArray` copies on every
  * call, which on a 16 MiB payload is one full duplicate per iteration
  * per direction; the protected fields of the base class expose the
  * same bytes in place.
  */
final class Acc(cap: Int) extends ByteArrayOutputStream(cap):
  /** The backing array; only the first [[len]] bytes are live. */
  def raw: Array[Byte] = buf

  /** Live byte count. */
  def len: Int = count

/** A failed session call: which entry failed, and with what. */
final case class PumpFail(what: String, err: ItbError)

object Worker:

  /** Pattern the binding's error detail carries: the status code and,
    * behind it, the library's own diagnostic sentence.
    */
  private val DetailRe: Regex = "(?s)^itb: status=(-?\\d+)(?:: (.*))?$".r

  /** Renders a binding error the way every implementation reports a
    * failed library call: `status <code>: <last error>`. No wording of
    * a status code is composed here — the library's diagnostic already
    * opens with the class of failure and, where there is one, the
    * specific case, so it is printed as it arrived.
    *
    * Scala-specific. That diagnostic is the error value's `detail`
    * field, which carries the message the Java layer composed behind a
    * fixed prefix, so the prefix is where it is recovered from.
    */
  def detail(e: ItbError): String =
    DetailRe.findFirstMatchIn(e.detail) match
      case Some(m) => s"status ${m.group(1)}: ${Option(m.group(2)).getOrElse("")}"
      case None    => if e.detail.nonEmpty then e.detail else s"status ${e.code}"

  /** Records the worker's error text (first error wins) and requests a
    * stop of the whole run.
    */
  def fail(r: RunState, id: Int, text: String): Unit =
    r.workers(id).setError(text)
    r.stop = true

  /** Pump loop. The Go harness hands ITB an io.Reader / io.Writer pair
    * and ITB drives the chunk loop internally; the C ABI has no reader
    * / writer entry, so the caller drives it: open a session, feed
    * slices of at most 1 MiB, drain whatever the session has produced
    * after every write (a read before end never blocks), end, then
    * drain until the session reports finished (after end, a read on an
    * empty spool blocks until the terminal bytes arrive). The whole
    * produced output lands in the worker's reusable accumulator. The
    * loop is written here rather than delegated to the binding's pump
    * convenience so it stands in the utility, at the same place, in
    * every language. On failure the result names the failing call and
    * carries its error.
    *
    * Scala-specific. The binding's session write takes a whole array,
    * so a full-width slice is fed from the worker's reusable feed
    * buffer and only the short final slice is cut fresh.
    */
  private def pump(
      pipe: Pipeline,
      encrypt: Boolean,
      src: Array[Byte],
      srcLen: Int,
      acc: Acc,
      scratch: Array[Byte],
      feed: Array[Byte]
  ): Option[PumpFail] =
    acc.reset()
    var es: EncryptStream = null
    var ds: DecryptStream = null
    val begun = if encrypt then pipe.beginEncryptStream() else pipe.beginDecryptStream()
    begun match
      case Left(e)                   => return Some(PumpFail("StreamBegin", e))
      case Right(s: EncryptStream)   => es = s
      case Right(s: DecryptStream)   => ds = s
    try
      // Scala-specific. The two session directions are distinct final
      // classes with no common supertype on the binding's surface, so
      // every call below branches on which side is open.
      def writeSlice(a: Array[Byte]): Either[ItbError, Unit] =
        if es != null then es.write(a) else ds.write(a)
      def readSlice(): Either[ItbError, io.github.everanium.itb3.scala.ReadResult] =
        if es != null then es.read(scratch) else ds.read(scratch)

      var off = 0
      while off < srcLen do
        val n = math.min(Main.PumpSlice, srcLen - off)
        val slice =
          if n == Main.PumpSlice then
            System.arraycopy(src, off, feed, 0, n)
            feed
          else Arrays.copyOfRange(src, off, off + n)
        writeSlice(slice) match
          case Left(e)  => return Some(PumpFail("StreamWrite", e))
          case Right(_) => ()
        var draining = true
        while draining do
          readSlice() match
            case Left(e) => return Some(PumpFail("StreamRead", e))
            case Right(res) =>
              if res.count == 0 then draining = false
              else acc.write(scratch, 0, res.count)
        off += Main.PumpSlice

      (if es != null then es.end() else ds.end()) match
        case Left(e)  => return Some(PumpFail("StreamEnd", e))
        case Right(_) => ()

      var finished = false
      while !finished do
        readSlice() match
          case Left(e) => return Some(PumpFail("StreamRead", e))
          case Right(res) =>
            acc.write(scratch, 0, res.count)
            finished = res.finished
      None
    finally
      if es != null then es.close()
      if ds != null then ds.close()

  /** First offset at which the two ranges differ; the shorter length
    * when one is a prefix of the other.
    */
  private def firstDifference(a: Array[Byte], aLen: Int, b: Array[Byte], bLen: Int): Int =
    val n = math.min(aLen, bLen)
    var i = 0
    while i < n do
      if a(i) != b(i) then return i
      i += 1
    n

  /** Up to 16 bytes of buf from off as lowercase hex, or "-" when buf
    * has no bytes there.
    */
  private def hexWindow(buf: Array[Byte], len: Int, off: Int): String =
    if off >= len then return "-"
    val n = math.min(16, len - off)
    val sb = StringBuilder()
    var i = 0
    while i < n do
      val v = buf(off + i) & 0xff
      sb.append(Character.forDigit(v >>> 4, 16)).append(Character.forDigit(v & 0xf, 16))
      i += 1
    sb.toString

  private def equalBytes(a: Array[Byte], aLen: Int, b: Array[Byte], bLen: Int): Boolean =
    if aLen != bLen then return false
    var i = 0
    while i < aLen do
      if a(i) != b(i) then return false
      i += 1
    true

  /** Records a worker error for a failed cipher call. */
  private def cipherFail(
      r: RunState,
      id: Int,
      iter: Long,
      shape: Shape,
      direction: String,
      what: Option[String],
      e: ItbError
  ): Unit =
    val head = s"g$id iter $iter shape=${shape.label}: $direction"
    fail(r, id, what.fold(s"$head: ${detail(e)}")(w => s"$head: $w: ${detail(e)}"))

  /** One iteration. In order: refill the plaintext under rotating mode;
    * take the read lock; pick the surface; encrypt (timed); decrypt
    * (timed); compare the round-trip with the plaintext; bump the
    * counters; release the lock. The whole round-trip runs under the
    * read lock so handle-mutating maintenance (rekey, blob reopen)
    * never lands between an encrypt and its matching decrypt —
    * maintenance runs after this returns, from the worker loop. False
    * after recording a worker error.
    */
  private def iterate(r: RunState, w: WorkerState, iter: Long): Boolean =
    val c = r.workers(w.id)
    if w.payloadMode == PayloadMode.Rotating &&
      !Payload.fill(PayloadMode.Rotating, w.seeded, w.rng, w.plaintext)
    then
      fail(r, w.id, s"g${w.id} iter $iter: payload refill: csprng")
      return false

    r.pipesLock.readLock().lock()
    try
      // Shape dispatch. message is one whole-buffer call on the Single
      // Message Pipeline; stream_one_shot is one whole-buffer call on
      // the streaming Pipeline (the C ABI's ITB_Triple_EncryptStream,
      // which routes to the same one-shot stream entry the Go
      // harness calls by name); stream opens a session on the same
      // streaming Pipeline and drives the chunk loop from here. Under
      // both the three rotate by iteration number so the session path
      // and the whole-buffer path alternate on one handle inside every
      // worker — the cross-path state-reuse hazard this harness exists
      // to catch.
      var shape = r.cfg.shape
      if shape == Shape.Both then
        shape = (iter % 3) match
          case 0 => Shape.Stream
          case 1 => Shape.Message
          case _ => Shape.StreamOneShot

      // Scala-specific. The message and one-shot entries return a fresh
      // array per call that the collector reclaims at the end of the
      // iteration; the pump accumulators are the worker's own and are
      // reused. `got` / `gotLen` hold the round-trip output for either
      // posture, so one comparison below serves both.
      var got: Array[Byte] = null
      var gotLen = 0
      var t0 = 0L
      shape match
        case Shape.Stream =>
          val pipe = r.pipes.stream.get
          t0 = System.nanoTime()
          pump(pipe, true, w.plaintext, w.plaintext.length, w.wire, w.scratch, w.feed) match
            case Some(f) =>
              cipherFail(r, w.id, iter, shape, "encrypt", Some(f.what), f.err)
              return false
            case None => ()
          c.addEncrypt(System.nanoTime() - t0)
          t0 = System.nanoTime()
          pump(pipe, false, w.wire.raw, w.wire.len, w.plain, w.scratch, w.feed) match
            case Some(f) =>
              cipherFail(r, w.id, iter, shape, "decrypt", Some(f.what), f.err)
              return false
            case None => ()
          c.addDecrypt(System.nanoTime() - t0)
          got = w.plain.raw
          gotLen = w.plain.len

        case Shape.StreamOneShot =>
          val pipe = r.pipes.stream.get
          t0 = System.nanoTime()
          val wire = pipe.encryptStreamOneShot(w.plaintext) match
            case Left(e) =>
              cipherFail(r, w.id, iter, shape, "encrypt", None, e)
              return false
            case Right(v) => v
          c.addEncrypt(System.nanoTime() - t0)
          t0 = System.nanoTime()
          got = pipe.decryptStreamOneShot(wire) match
            case Left(e) =>
              cipherFail(r, w.id, iter, shape, "decrypt", None, e)
              return false
            case Right(v) => v
          c.addDecrypt(System.nanoTime() - t0)
          gotLen = got.length

        case _ =>
          val pipe = r.pipes.msg.get
          t0 = System.nanoTime()
          val wire = pipe.encryptMessage(w.plaintext) match
            case Left(e) =>
              cipherFail(r, w.id, iter, shape, "encrypt", None, e)
              return false
            case Right(v) => v
          c.addEncrypt(System.nanoTime() - t0)
          t0 = System.nanoTime()
          got = pipe.decryptMessage(wire) match
            case Left(e) =>
              cipherFail(r, w.id, iter, shape, "decrypt", None, e)
              return false
            case Right(v) => v
          c.addDecrypt(System.nanoTime() - t0)
          gotLen = got.length

      // Failure model. A cipher call that returns a non-OK status is a
      // worker error: it is recorded, the run is asked to stop, the
      // other workers finish their in-flight iteration, and the error
      // is listed in the summary with the FAIL verdict. A round-trip
      // that returns OK with different bytes is a data mismatch: the
      // process terminates here, without summary or cleanup, because
      // the Pipeline state that produced the wrong bytes is the
      // evidence and nothing that runs afterwards may touch it.
      // Scala-specific: Runtime.halt is the exit that runs neither the
      // shutdown hooks nor the Cleaner registrations behind the Java
      // layer's handles, which is the point — a cleaner-driven free
      // would release the very state the operator is meant to inspect.
      val want = w.plaintext
      if !equalBytes(want, want.length, got, gotLen) then
        val off = firstDifference(want, want.length, got, gotLen)
        System.err.println(
          s"loop: DATA MISMATCH g${w.id} iter $iter shape=${shape.label}: " +
            s"want ${want.length} bytes, got $gotLen bytes, " +
            s"first difference at offset $off: " +
            s"want ${hexWindow(want, want.length, off)} got ${hexWindow(got, gotLen, off)}"
        )
        System.err.flush()
        System.out.flush()
        java.lang.Runtime.getRuntime.halt(3)

      c.addIteration(want.length.toLong, gotLen.toLong)
      true
    finally r.pipesLock.readLock().unlock()

  /** Marks this worker returned; the last one to return stamps the
    * finish instant and wakes main.
    */
  private def done(r: RunState): Unit =
    r.doneLock.lock()
    try
      r.active -= 1
      if r.active == 0 then
        r.finishNanos = System.nanoTime()
        r.doneCond.signalAll()
    finally r.doneLock.unlock()

  /** The worker thread body: one warmup iteration, the warmup barrier,
    * then the main loop until a stop is requested or the fixed
    * per-worker iteration budget (warmup included) is spent. A failing
    * warmup still passes both barriers so the launcher never waits on a
    * worker that has already given up.
    */
  def run(r: RunState, w: WorkerState): Unit =
    // Warmup iteration — counted in the totals; its completion feeds
    // the post-warmup baselines.
    val ok = iterate(r, w, 0L)
    try
      r.warmupDone.await()
      r.release.await()
    catch
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        done(r)
        return
      case _: BrokenBarrierException =>
        done(r)
        return
    if !ok then
      done(r)
      return

    var iter = 1L
    var running = true
    while running do
      if (r.cfg.iterations > 0 && iter >= r.cfg.iterations) || r.stop then running = false
      else if !iterate(r, w, iter) then running = false
      else if !Ops.maintenance(r, w.id, iter) then running = false
      else iter += 1
    done(r)
