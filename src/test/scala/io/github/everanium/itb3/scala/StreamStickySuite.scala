// A decrypt session fed a tampered wire fails with a sticky MAC
// failure. Uses a position probe rather than a single bit flip
// because the over-sized container carries CSPRNG residue in the
// non-payload area — a flip that lands inside the residue is
// architecturally inert (residue is not payload) and the session
// finishes clean. Probing 32 evenly-spaced positions makes the
// all-residue probability negligible; the first position that
// surfaces an error must give Status.MacFailure and remain sticky on
// subsequent reads.

package io.github.everanium.itb3.scala

import scala.util.Using
import scala.util.boundary

class StreamStickySuite extends ItbSuite:

  test("tampered wire sticky failure") {
    Using.resource(ok(Pipeline.init("streaming-aead-triple-mac-v1"))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) {
        receiver =>
          val plain = Array.tabulate(65_536)(i => (i % 227).toByte)
          val baseWire = ok(sender.encryptStreamOneShot(plain))
          assert(
            baseWire.length > 128,
            s"wire too short to place a distributed probe: ${baseWire.length} bytes"
          )

          val probes = 32
          // Evenly spread through the wire body; skip the first /
          // last 16 bytes so a hit against the outer envelope
          // framing does not muddy the observation.
          val bodyStart = 16
          val bodyEnd = baseWire.length - 16
          val stride = (bodyEnd - bodyStart) / probes

          boundary:
            for probe <- 0 until probes do
              val idx = bodyStart + probe * stride
              val wire = baseWire.clone()
              wire(idx) = (wire(idx) ^ 0x01).toByte

              Using.resource(ok(receiver.beginDecryptStream())) { session =>
                // Ignore write / end status — the failure may
                // surface on either side or only on the drain that
                // follows.
                val _ = session.write(wire)
                val _ = session.end()

                val buf = new Array[Byte](4096)
                var firstErr: Option[ItbError] = None
                var finishedClean = false
                while firstErr.isEmpty && !finishedClean do
                  session.read(buf) match
                    case Right(r) => finishedClean = r.finished
                    case Left(e)  => firstErr = Some(e)

                firstErr match
                  case None =>
                  // Residue hit at this offset — try the next probe.
                  case Some(e) =>
                    assertEquals(
                      e.status,
                      Status.MacFailure,
                      s"expected MAC failure on tampered wire at probe $probe (byte $idx)"
                    )
                    // Sticky: a subsequent read reports the same
                    // status.
                    assertEquals(err(session.read(buf)).status, e.status)
                    boundary.break()
              }
            fail(
              s"no probe among $probes evenly-spaced positions surfaced a MAC " +
                "failure — either the probe pattern is degenerate or " +
                "authentication is not covering the wire body it should"
            )
      }
    }
  }
