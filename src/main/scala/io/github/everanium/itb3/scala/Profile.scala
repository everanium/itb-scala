// Profile record — the Java binding's typed view of the Triple
// profile JSON object, re-exported under the Scala package.
//
// The record is a plain data holder plus a JSON codec over the wire
// keys. nonce_bits and barrier_fill are inspection-only —
// Pipeline.inspect populates them from the blob's runtime globals and
// Pipeline.lookup leaves both null. No semantic validation happens on
// the JVM side — every field rule is enforced by Go at
// Pipeline.register / Pipeline.load time and surfaces as ItbError.

package io.github.everanium.itb3.scala

/** A Triple Pipeline profile record — the type [[Pipeline.inspect]]
  * and [[Pipeline.lookup]] return and [[Pipeline.register]] accepts.
  * Fluent setters chain (`Profile().mode("singlemsg-nomac").width(512)`).
  */
type Profile = io.github.everanium.itb3.Profile

object Profile:
  /** An empty record; populate through the fluent setters. */
  def apply(): Profile = new io.github.everanium.itb3.Profile()
