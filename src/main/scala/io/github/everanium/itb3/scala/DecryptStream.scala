// Incremental decrypt session — the mirror of EncryptStream.

package io.github.everanium.itb3.scala

import java.io.OutputStream

import io.github.everanium.itb3.DecryptStream as JDecryptStream

import ItbError.attempt

/** Incremental decrypt session: wire in through [[write]], plaintext
  * out through [[read]] / [[copyTo]]. A session is a dumb byte pump
  * — all chunking, MAC, envelope, and wire-format decisions stay
  * inside libitb3. `close()` cancels the session and frees the
  * Go-side state; the session keeps `parent` reachable while it is
  * live.
  */
final class DecryptStream private[scala] (
    val parent: Pipeline,
    private val underlying: JDecryptStream
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

  /** Lazily maps input wire chunks to output plaintext chunks
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
