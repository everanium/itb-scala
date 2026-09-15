// Error value shared by every fallible call in the binding.

package io.github.everanium.itb3.scala

import io.github.everanium.itb3.ItbException

/** Error surfaced through the `Either` results of the binding.
  *
  * `status` carries the structural code; `code` is the raw integer,
  * preserved even when it has no named [[Status]] constant; `detail`
  * carries the formatted diagnostic captured by the Java binding
  * immediately after the failing call — it embeds the process-global
  * `ITB_LastError` text (last-write-wins — under concurrent use the
  * text may belong to a different call; the status code is always
  * attributable).
  *
  * The class extends `RuntimeException` so the same value can travel
  * through throw-based interop layers (`Try`, fs2 / Cats Effect
  * error channels, the `Iterator` adapters) without a second error
  * type.
  */
final case class ItbError(status: Status, code: Int, detail: String)
    extends RuntimeException(
      if detail.nonEmpty then detail else s"itb: status=$code ($status)"
    )

object ItbError:

  /** Converts the Java binding's exception, preserving the raw
    * code.
    */
  private[scala] def from(e: ItbException): ItbError =
    ItbError(Status.fromCode(e.rawCode), e.rawCode, Option(e.getMessage).getOrElse(""))

  /** Runs `body`, mapping the Java binding's exception into the
    * `Either` error channel. Non-ITB exceptions propagate.
    */
  private[scala] def attempt[A](body: => A): Either[ItbError, A] =
    try Right(body)
    catch case e: ItbException => Left(from(e))
