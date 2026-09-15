// Process-wide Go runtime knobs plus the library version string.

package io.github.everanium.itb3.scala

import io.github.everanium.itb3.Runtime as JRuntime

/** Accessors for the libitb3 process-wide Go runtime knobs and the
  * library version. The knobs are readable at libitb3 load time via
  * env vars (`ITB_GOMEMLIMIT`, `ITB_GOGC`) and adjustable at any
  * time programmatically; a setter wins over the env var.
  */
object Runtime:

  /** The Scala binding's own version. */
  val BindingVersion: String = "0.5.1"

  /** Sets the Go runtime's soft heap limit in bytes and returns the
    * previous limit. A negative value queries without changing.
    */
  def setMemoryLimit(bytes: Long): Long = JRuntime.setMemoryLimit(bytes)

  /** Sets the Go GC trigger percentage and returns the previous
    * value. A negative value queries without changing.
    */
  def setGCPercent(pct: Int): Int = JRuntime.setGCPercent(pct)

  /** Returns the libitb3 library version string. */
  def version: String = JRuntime.version()
