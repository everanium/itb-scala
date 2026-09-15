// Incremental encrypt session plus the shared chunk-iterator
// adapter used by both stream directions.

package io.github.everanium.itb3.scala

import java.io.OutputStream
import java.util.Arrays

import io.github.everanium.itb3.EncryptStream as JEncryptStream

import ItbError.attempt

/** Incremental encrypt session: plaintext in through [[write]], wire
  * out through [[read]] / [[copyTo]]. A session is a dumb byte pump
  * — all chunking, MAC, envelope, and wire-format decisions stay
  * inside libitb3. `close()` cancels the session and frees the
  * Go-side state; the session keeps `parent` reachable while it is
  * live.
  */
final class EncryptStream private[scala] (
    val parent: Pipeline,
    private val underlying: JEncryptStream
) extends AutoCloseable:

  /** Feeds bytes into the session. Blocks until the cipher chain
    * accepts them; errors are sticky.
    */
  def write(chunk: Array[Byte]): Either[ItbError, Unit] =
    attempt(underlying.write(chunk))

  /** Signals end-of-input. Idempotent; [[write]] after end fails
    * with [[Status.BadInput]].
    */
  def end(): Either[ItbError, Unit] = attempt(underlying.end())

  /** Drains up to `dst.length` produced bytes. Partial drains are
    * normal; before [[end]] a drain on an empty spool returns a zero
    * count without blocking, after [[end]] it blocks until the
    * terminal bytes arrive or the session errors.
    */
  def read(dst: Array[Byte]): Either[ItbError, ReadResult] =
    attempt(ReadResult(underlying.read(dst), underlying.isFinished()))

  /** Calls [[end]] (idempotent) and writes every remaining output
    * byte to `destination`.
    */
  def copyTo(destination: OutputStream): Either[ItbError, Unit] =
    attempt {
      underlying.end()
      val buf = new Array[Byte](StreamAdapters.DrainChunk)
      while !underlying.isFinished() do
        val n = underlying.read(buf)
        if n > 0 then destination.write(buf, 0, n)
      destination.flush()
    }

  /** Lazily maps input plaintext chunks to output wire chunks
    * through this session: each pulled output chunk feeds and drains
    * the session as needed, calling [[end]] once the input is
    * exhausted. Single pass — traverse the returned iterator at most
    * once and do not mix it with direct [[write]] / [[read]] calls.
    * A libitb3 failure surfaces as a thrown [[ItbError]] (it is a
    * `RuntimeException`), which fs2 / ZIO / Cats Effect adapters can
    * lift into their error channels.
    */
  def transform(chunks: Iterator[Array[Byte]]): Iterator[Array[Byte]] =
    StreamAdapters.transform(write, () => end(), read, chunks)

  /** [[transform]] materialized as a memoizing `LazyList`. */
  def toLazyList(chunks: Iterator[Array[Byte]]): LazyList[Array[Byte]] =
    LazyList.from(transform(chunks))

  /** Cancels the session and frees the Go-side state. Idempotent. */
  override def close(): Unit = underlying.close()

/** Shared plumbing for the two session directions. */
private[scala] object StreamAdapters:

  /** Feed / drain block size used by the iterator adapter and the
    * `copyTo` drain loops.
    */
  private[scala] val DrainChunk = 1 << 20

  /** Direction-agnostic chunk-iterator pump: pulls input chunks on
    * demand, feeds them through `write`, drains produced output, and
    * finishes with `end` + terminal drain once the input is
    * exhausted. Emits only non-empty output chunks.
    */
  private[scala] def transform(
      write: Array[Byte] => Either[ItbError, Unit],
      end: () => Either[ItbError, Unit],
      read: Array[Byte] => Either[ItbError, ReadResult],
      input: Iterator[Array[Byte]]
  ): Iterator[Array[Byte]] = new Iterator[Array[Byte]]:
    private val buf = new Array[Byte](DrainChunk)
    private var pending: Option[Array[Byte]] = None
    private var ended = false
    private var finished = false

    private def orThrow[A](result: Either[ItbError, A]): A =
      result.fold(e => throw e, identity)

    // Advances until an output chunk is pending or the session
    // output is complete.
    private def advance(): Unit =
      while pending.isEmpty && !finished do
        val r = orThrow(read(buf))
        if r.count > 0 then pending = Some(Arrays.copyOf(buf, r.count))
        if r.finished then finished = true
        else if r.count == 0 && !ended then
          if input.hasNext then orThrow(write(input.next()))
          else
            orThrow(end())
            ended = true
        // After end, an empty drain blocks inside libitb3 until the
        // terminal bytes arrive — the loop simply reads again.

    def hasNext: Boolean =
      advance()
      pending.isDefined

    def next(): Array[Byte] =
      advance()
      val out = pending.getOrElse(throw new NoSuchElementException("stream output exhausted"))
      pending = None
      out
