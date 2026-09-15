// Error-mapping surface: opaque-string relay, closed Pipeline,
// duplicate profile registration (with an 8-entry mixed
// constellation), unknown lookup, maxWorkers on a closed handle.

package io.github.everanium.itb3.scala

import scala.util.Using

class ErrorsSuite extends ItbSuite:

  test("unknown profile is UnknownProfile with diagnostic") {
    val e = err(Pipeline.init("no-such-profile"))
    assertEquals(e.status, Status.UnknownProfile)
    assert(e.getMessage.nonEmpty)
  }

  test("unknown lookup name is UnknownProfile") {
    val e = err(Pipeline.lookup("no-such-profile"))
    assertEquals(e.status, Status.UnknownProfile)
  }

  test("maxWorkers on a closed pipeline reports TripleClosed") {
    Using.resource(ok(Pipeline.init("singlemsg-triple-mac-v1"))) { pipe =>
      ok(pipe.closeSession())
      val e = err(pipe.maxWorkers(2))
      assertEquals(e.status, Status.TripleClosed)
    }
  }

  test("unknown opts key is BadInput") {
    // Typoed key (lowercase s) — Go rejects unknown keys.
    val opts = Opts.empty.withRaw("chunksize", "4096")
    val e = err(Pipeline.init("singlemsg-triple-mac-v1", opts))
    assertEquals(e.status, Status.BadInput)
  }

  test("closed pipeline reports TripleClosed") {
    Using.resource(ok(Pipeline.init("singlemsg-triple-mac-v1"))) { pipe =>
      ok(pipe.closeSession())
      ok(pipe.closeSession()) // idempotent
      val e = err(pipe.encryptMessage("payload".getBytes("UTF-8")))
      assertEquals(e.status, Status.TripleClosed)
    }
  }

  test("register mixed then duplicate") {
    // 8-entry width-256 mixed constellation, layers off.
    val profile = Profile()
      .mode("singlemsg-nomac")
      .width(256)
      .hashes(
        "blake3", "blake2s", "areion256", "blake2b256",
        "chacha20", "blake3", "blake2s", "areion256"
      )
      .keyBits(1024)
      .parallax(false)
      .wrapper(false)
    ok(Pipeline.register("scala-binding-test-mixed", profile))

    // The registered profile round-trips.
    Using.resource(ok(Pipeline.init("scala-binding-test-mixed"))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) { receiver =>
        val plain = "custom profile".getBytes("UTF-8")
        val wire = ok(sender.encryptMessage(plain))
        assert(ok(receiver.decryptMessage(wire)).sameElements(plain))
      }
    }

    // Duplicate name is a distinct status.
    val e = err(Pipeline.register("scala-binding-test-mixed", profile))
    assertEquals(e.status, Status.ProfileExists)
  }

  test("opaque primitive name relay") {
    // An unknown inner-hash name is relayed to Go and rejected there
    // — the binding performs no name validation of its own.
    val opts = Opts.empty.withInnerHash("no-such-hash")
    val e = err(Pipeline.init("singlemsg-triple-mac-v1", opts))
    assert(e.status != Status.Ok)
  }

  test("per-call innerHashes override round-trips") {
    // The single-primitive width-512 base profile takes an 8-slot
    // per-call MixedHashes override (Go-side Opts.MixedHashes, wired
    // through the innerHashes= opts key). Round-trip proves the typed
    // helper's comma-join lands in the Go parser correctly.
    val senderOpts = Opts.empty.withInnerHashes(
      "areion512", "blake2b512", "areion512", "blake2b512",
      "areion512", "blake2b512", "areion512", "blake2b512",
    )
    val receiverOpts = Opts.empty.withInnerHashes(
      "areion512", "blake2b512", "areion512", "blake2b512",
      "areion512", "blake2b512", "areion512", "blake2b512",
    )
    Using.resource(ok(Pipeline.init("singlemsg-triple-mac-v1", senderOpts))) { sender =>
      Using.resource(ok(Pipeline.load(ok(sender.save())))) {
        receiver =>
          val plain = "per-call inner-hashes override round-trip payload".getBytes("UTF-8")
          val wire = ok(sender.encryptMessage(plain))
          assert(ok(receiver.decryptMessage(wire)).sameElements(plain))
      }
    }
  }
