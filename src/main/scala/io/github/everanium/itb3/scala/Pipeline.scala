// Idiomatic Scala lifetime + error surface over the Java binding's
// Triple Pipeline.

package io.github.everanium.itb3.scala

import java.io.{InputStream, OutputStream}

import scala.jdk.CollectionConverters.*

import io.github.everanium.itb3.Pipeline as JPipeline

import ItbError.attempt

/** Result of one stream drain call: `count` bytes were placed into
  * the destination buffer; `finished` marks the session output as
  * complete.
  */
final case class ReadResult(count: Int, finished: Boolean)

/** A Triple Pipeline session.
  *
  * [[save]] exports the self-describing session blob the receiver
  * feeds to [[Pipeline.load]] / [[Pipeline.loadF]]; [[rekey]]
  * refreshes it. `close()` releases the native handle (libitb3 zeroes
  * key material internally), so the class composes with
  * `scala.util.Using.resource`; the Java binding's finalization
  * backstop reclaims an unclosed Pipeline.
  *
  * Every fallible operation returns `Either[ItbError, _]`; the
  * for-comprehension is the intended composition style. Callers
  * preferring exceptions can `fold(throw _, identity)` — the error
  * value is a `RuntimeException`.
  *
  * Streaming-decrypt caveat: chunked Streaming AEAD verifies per
  * chunk, so plaintext of verified chunks is released before a later
  * chunk can fail authentication.
  */
final class Pipeline private (private[scala] val underlying: JPipeline)
    extends AutoCloseable:

  /** The current self-describing session blob: the bytes [[Pipeline.init]]
    * produced, the bytes [[Pipeline.load]] re-marshalled, or the
    * bytes of the latest [[rekey]].
    */
  def save(): Either[ItbError, Array[Byte]] = attempt(underlying.save())

  /** Writes [[save]] to `path` inside the library with mode 0600;
    * the containing directory must exist.
    */
  def saveF(path: String): Either[ItbError, Unit] = attempt(underlying.saveF(path))

  /** Sets the worker cap for every subsequent cipher call. `n` is
    * clamped, never rejected: `n <= 0` selects auto (CPU count),
    * `n > 256` is treated as 256. Only the handle statuses fail.
    */
  def maxWorkers(n: Int): Either[ItbError, Unit] = attempt(underlying.maxWorkers(n))

  /** Rotates the parallax + wrapper masters and returns the fresh
    * session blob (also available through [[save]]). Must not run
    * concurrently with cipher calls or open stream sessions on the
    * same Pipeline.
    */
  def rekey(permMaster: Array[Byte], wrapMaster: Array[Byte]): Either[ItbError, Array[Byte]] =
    attempt(underlying.rekey(permMaster, wrapMaster))

  /** Zeroes the Pipeline's key material and marks it closed.
    * Idempotent; subsequent cipher calls fail with
    * [[Status.TripleClosed]]. The native handle itself is released
    * by [[close]].
    */
  def closeSession(): Either[ItbError, Unit] = attempt(underlying.destroy())

  /** Single Message encrypt: one call, one self-contained wire. */
  def encryptMessage(plaintext: Array[Byte]): Either[ItbError, Array[Byte]] =
    attempt(underlying.encryptMessage(plaintext))

  /** Receive-side counterpart of [[encryptMessage]]. */
  def decryptMessage(wire: Array[Byte]): Either[ItbError, Array[Byte]] =
    attempt(underlying.decryptMessage(wire))

  /** One-shot stream encrypt for callers holding the whole plaintext
    * in memory. For bounded-memory streaming use
    * [[beginEncryptStream]] / [[encryptStreamPump]].
    */
  def encryptStreamOneShot(plaintext: Array[Byte]): Either[ItbError, Array[Byte]] =
    attempt(underlying.encryptStreamOneShot(plaintext))

  /** Receive-side counterpart of [[encryptStreamOneShot]]. */
  def decryptStreamOneShot(wire: Array[Byte]): Either[ItbError, Array[Byte]] =
    attempt(underlying.decryptStreamOneShot(wire))

  /** Opens an incremental encrypt session (plaintext in, wire out).
    * The session holds a reference to this Pipeline, keeping it
    * reachable while the session is live.
    */
  def beginEncryptStream(): Either[ItbError, EncryptStream] =
    attempt(new EncryptStream(this, underlying.encryptStream()))

  /** Opens an incremental decrypt session (wire in, plaintext out).
    * The session holds a reference to this Pipeline, keeping it
    * reachable while the session is live.
    */
  def beginDecryptStream(): Either[ItbError, DecryptStream] =
    attempt(new DecryptStream(this, underlying.decryptStream()))

  /** Pumps `source` through an encrypt session into `destination`
    * with bounded memory: feed a block, drain available wire,
    * repeat; end + final drain on source EOF. The session is freed
    * on return.
    */
  def encryptStreamPump(source: InputStream, destination: OutputStream): Either[ItbError, Unit] =
    attempt(underlying.encryptStreamPump(source, destination))

  /** Receive-side counterpart of [[encryptStreamPump]]. */
  def decryptStreamPump(source: InputStream, destination: OutputStream): Either[ItbError, Unit] =
    attempt(underlying.decryptStreamPump(source, destination))

  /** Releases the native handle. Idempotent. */
  override def close(): Unit = underlying.close()

object Pipeline:

  /** Constructs a fresh Pipeline against the named profile. */
  def init(profile: String, opts: Opts = Opts.empty): Either[ItbError, Pipeline] =
    attempt(new Pipeline(JPipeline.init(profile, opts.toJava)))

  /** Reconstructs a Pipeline from a blob produced by
    * [[Pipeline.save]] or [[Pipeline.rekey]], using the
    * blob-embedded masters. The blob's embedded profile record is the
    * sole structural source. See [[loadWithMasters]] to override the
    * masters.
    */
  def load(blob: Array[Byte]): Either[ItbError, Pipeline] =
    attempt(new Pipeline(JPipeline.load(blob)))

  /** [[load]] with explicit (non-empty) parallax + wrapper masters
    * overriding the blob-embedded ones; both must be supplied.
    */
  def loadWithMasters(
      blob: Array[Byte],
      permMaster: Array[Byte],
      wrapMaster: Array[Byte]
  ): Either[ItbError, Pipeline] =
    attempt(new Pipeline(JPipeline.load(blob, permMaster, wrapMaster)))

  /** [[load]] for a blob stored in a file; the file is read inside
    * the library.
    */
  def loadF(path: String): Either[ItbError, Pipeline] =
    attempt(new Pipeline(JPipeline.loadF(path)))

  /** [[loadF]] with explicit (non-empty) parallax + wrapper masters
    * overriding the blob-embedded ones; both must be supplied.
    */
  def loadFWithMasters(
      path: String,
      permMaster: Array[Byte],
      wrapMaster: Array[Byte]
  ): Either[ItbError, Pipeline] =
    attempt(new Pipeline(JPipeline.loadF(path, permMaster, wrapMaster)))

  /** Decodes the blob's embedded profile record without opening a
    * Pipeline. No registry read, no primitive probe.
    */
  def inspect(blob: Array[Byte]): Either[ItbError, Profile] =
    attempt(JPipeline.inspect(blob))

  /** Registers `profile` under `name` so subsequent [[init]] /
    * [[lookup]] calls resolve it. Every field rule is validated by
    * Go; a duplicate name fails with [[Status.ProfileExists]].
    */
  def register(name: String, profile: Profile): Either[ItbError, Unit] =
    attempt(JPipeline.register(name, profile))

  /** Looks up a registered profile (shipped or [[register]]ed) by
    * name; an unknown name fails with [[Status.UnknownProfile]].
    */
  def lookup(name: String): Either[ItbError, Profile] =
    attempt(JPipeline.lookup(name))

  /** The sorted names of every registered profile. */
  def profiles(): Either[ItbError, List[String]] =
    attempt(JPipeline.profiles().asScala.toList)
