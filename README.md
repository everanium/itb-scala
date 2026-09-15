## ITB Scala Binding

> **Security notice.** ITB is an experimental symmetric cipher construction without prior peer review, independent cryptanalysis, or formal certification. The construction's security properties have **not been verified** by independent cryptographers or mathematicians.
>
> PRF-grade hash functions are **required**. No warranty is provided.

**No bespoke cryptography.** ITB introduces no cryptographic primitive of its own — no custom S-box, permutation, or round function. It is a construction over existing primitives, much as PGP composes standard ciphers rather than defining one. Such constructions are not the object of algorithm-level cryptographic certification: national regimes (NIST CAVP/FIPS in the US, GOST/FSB in Russia, OSCCA's SM-series in China, IC3S in India, SOG-IS/EUCC and national lists in the EU, ASD's ISM in Australia, CRYPTREC in Japan, KCMVP in South Korea) certify **primitives** and the **modules** built on them, not compositional schemes. Eligibility for regulated use is therefore inherited from the primitives ITB is configured with, not conferred by ITB itself.

Thin idiomatic layer over the Java binding
([`../java/`](https://github.com/everanium/itb/tree/main/bindings/java/)) — plain JVM bytecode interop, no FFI hop of
its own; the Java binding carries the JNI shim over the libitb3
`ITB_Triple_*` surface. Every hash-name / MAC-name / cipher-name /
profile-name is an opaque string passed through to Go for
validation; the binding carries no ITB construction logic.

The public surface wraps the Java `Pipeline` / `Opts` in Scala 3
idiom: every fallible call returns `Either[ItbError, _]` (the error
value is also a `RuntimeException`, so throw-based interop stays a
one-liner), results are case classes, `Opts` is an immutable
chainable value, and `Pipeline` / stream sessions are `AutoCloseable`
for `scala.util.Using.resource`. The stream sessions additionally
expose `Iterator[Array[Byte]] => Iterator[Array[Byte]]` /
`LazyList` chunk adapters for functional pipelines; an `fs2.Stream`
/ ZIO wrapper is a straightforward lift of the iterator adapter or
the `Either`-surface `write` / `end` / `read` calls into the effect
type of choice, and is deliberately left to the consumer so the
binding stays dependency-free.

## Prerequisites (Arch Linux)

```bash
sudo pacman -S go jdk17-openjdk gradle gcc sbt
```

Generic Linux: a Go toolchain, JDK 17+, Gradle, a C compiler (for
the Java binding's JNI shim), and sbt 1.9+.

## Build

The convenience driver builds the Java binding first (libitb3.so +
JNI shim + jars via `../java/build.sh`), then compiles the Scala
library, test, bench, and eitb projects:

```bash
./bindings/scala/build.sh
```

The Java jar is consumed as an sbt unmanaged jar from
`../java/build/libs/`; `ITB_JAVA_LIBS_DIR` overrides that location
(e.g. an installed copy of the Java binding).

## Library lookup order

Native resolution is inherited from the Java binding:

1. `ITB_JNI_PATH` environment variable (path to the JNI shim,
   `bindings/java/build/jni/libitb3_jni.so` for in-repo builds).
2. `System.loadLibrary("itb3_jni")` over `java.library.path`.

The shim locates `libitb3.so` through its RPATH (the repository
`dist/linux-amd64/` directory) or the OS loader path.

## Usage example

```scala
import scala.util.Using
import io.github.everanium.itb3.scala.{Opts, Pipeline}

Using.resource(Pipeline.init("singlemsg-triple-mac-v1").fold(throw _, identity)) { sender =>
  Using.resource(Pipeline.load(sender.save().fold(throw _, identity)).fold(throw _, identity)) { receiver =>
    val result =
      for
        wire  <- sender.encryptMessage("any text or binary data".getBytes("UTF-8"))
        plain <- receiver.decryptMessage(wire)
      yield plain
  }
}
// Or persist the session to disk and reopen it later:
//   sender.saveF("/path/session.blob")
//   val receiver = Pipeline.loadF("/path/session.blob").fold(throw _, identity)
```

`Opts` overrides the profile default at `init` (chunk size, outer
cipher, parallax on/off, wrapper on/off, MAC name, palette,
`maxWorkers`); the immutable case class chains `with*` calls. The
blob the receiver loads carries the resolved shape, so `load` takes
no opts:

```scala
val opts = Opts().withChunkSize(65536L).withWrapper(false)
val sender = Pipeline.init("singlemsg-triple-mac-v1", opts).fold(throw _, identity)
val receiver = Pipeline.load(sender.save().fold(throw _, identity)).fold(throw _, identity)
```

`Pipeline.rekey` rotates the parallax + wrapper masters mid-session
(the eight ITB seeds and MAC key are fixed for the session lifetime
by design) and returns the refreshed blob; the receiver picks up
the new masters through a fresh `save()` / `load` handshake:

```scala
val rotated = sender.rekey(Array.fill[Byte](32)(0x11), Array.fill[Byte](32)(0x22)).fold(throw _, identity)
val receiver2 = Pipeline.load(rotated).fold(throw _, identity)
```

For bounded-memory streaming, `encryptStreamPump` /
`decryptStreamPump` move any `java.io.InputStream` source into any
`java.io.OutputStream` sink through an incremental session. The
explicit `beginEncryptStream()` / `beginDecryptStream()` sessions
expose `write` / `end` / `read` for caller-driven loops plus the
`transform` / `toLazyList` chunk-iterator adapters:

```scala
Using.resource(pipe.beginEncryptStream().fold(throw _, identity)) { session =>
  val wireChunks: Iterator[Array[Byte]] = session.transform(plaintextChunks)
  wireChunks.foreach(sink)
}
```

Profile names, opts keys, and every primitive name are validated by
the Go side; a rejected string surfaces as a `Left(ItbError)`
carrying the [`Status`](src/main/scala/io/github/everanium/itb3/scala/Status.scala)
code plus the `ITB_LastError` diagnostic.

## Persisting sessions

The blob `save()` returns is self-describing: it carries the profile
record (the resolved pipeline shape) alongside the key material, so
a receiver reconstructs the session from the blob alone.

```scala
val blob = sender.save().fold(throw _, identity)          // current session blob
sender.saveF("/path/session.blob")                        // same bytes, written by the library (mode 0600)
val a = Pipeline.load(blob)                               // reopen from bytes
val b = Pipeline.loadF("/path/session.blob")              // reopen from a file
val c = Pipeline.loadWithMasters(blob, perm, wrap)        // reopen with a master override
val p: Either[ItbError, Profile] = Pipeline.inspect(blob) // metadata only, no Pipeline opened
```

Load works for blobs generated with shipped primitives (every entry
in the shipped catalogue). Blobs generated by Go programs that use
`hashes.Register` or `macs.Register` to install custom primitives
cannot be loaded through this binding — the receiver must use the Go
library directly and register the same custom primitive under the
same name before opening. Attempting to load such a blob through
this binding surfaces `Status.RecipePrimitiveUnknown`. A blob from an earlier wrap-layer
version surfaces `Status.BadInput`; a record that fails the profile field
rules surfaces `Status.BlobMalformedRecipe`.

The profile registry is reachable through the same `Profile`
record:

```scala
val names = Pipeline.profiles()                           // Either[ItbError, List[String]]
val shipped = Pipeline.lookup("singlemsg-triple-nomac-v1")
val custom = Profile()
  .mode("singlemsg-nomac").width(512).hash("areion512").keyBits(1024)
  .wrapper(false).parallax(false)
Pipeline.register("my-profile", custom)                  // validated by Go; duplicate -> ProfileExists
```

`Profile` is a plain record plus JSON codec — no validation happens
on the binding side. `inspect` / `lookup` return it; `register`
accepts it; an unknown name at `init` / `lookup` surfaces `Status.UnknownProfile`.

Runtime tuning: `pipeline.maxWorkers(n)` sets the worker cap for every
subsequent cipher call (`n <= 0` selects auto, `n > 256` is clamped
to 256); the receiver may pick its own worker cap after `load` — the
cap is per-machine and never written to the blob.

## Memory

Two process-wide knobs constrain Go runtime arena pacing, readable
at libitb3 load time via env vars (`ITB_GOMEMLIMIT`, `ITB_GOGC`) and
adjustable at any time programmatically. Pass `-1` to query without
changing:

```scala
import io.github.everanium.itb3.scala.Runtime as ItbRuntime

ItbRuntime.setMemoryLimit(4L * 1024 * 1024 * 1024)
ItbRuntime.setGCPercent(100)
```

## Testing

```bash
./bindings/scala/run_tests.sh
```

The harness builds everything, exports `ITB_JNI_PATH`, and invokes
`sbt test` (MUnit). The suite covers Single Message round trips per
shipped profile, stream pumps and the chunk-iterator adapters,
incremental sessions with pathological batch sizes, tampered-wire
failure stickiness, mid-flight cancellation, rekey, session persistence (save / load, saveF / loadF, inspect, lookup / profiles / register, maxWorkers),
and error mapping — surface parity checks; the deep
test suite lives in Go under the shipped tree.

## Benchmarking

```bash
./bindings/scala/run_bench.sh            # both shapes
./bindings/scala/run_bench.sh message    # Single Message shape only
./bindings/scala/run_bench.sh stream     # stream-pump shape only
```

Wall-clock micro-benches: `encryptMessage` and stream-pump
throughput at 1 MiB / 16 MiB / 64 MiB. Shape and budget are driven
by the `ITB_*` env vars listed in
`bench/src/main/scala/io/github/everanium/itb3/scala/bench/BenchUtil.scala`;
defaults match the root Go BENCH3.md pin.

## itb3 CLI

The Go core ships an openssl-style CLI utility
[`itb3`](https://github.com/everanium/itb/tree/main/cmd/itb3/) that generates session blobs on disk
(`itb3 genblob <mode> <hash> -o blob.json`); this binding reopens
such blobs via `Pipeline.loadF`. `itb3` also encrypts / decrypts
payloads directly on disk (`-i` / `-o`) or through stdin / stdout,
rotates outer masters, and inspects stored blobs. See
[`cmd/itb3/README.md`](https://github.com/everanium/itb/blob/main/cmd/itb3/README.md) for the full
subcommand reference.

## eitb utility

The `eitb/eitb` launcher compiles on first use, caches the runtime
classpath, and mirrors the shipped Go `tools/eitb` scope for shell
smoke tests:

```bash
cd bindings/scala
eitb/eitb version
eitb/eitb profiles
eitb/eitb encrypt singlemsg-triple-mac-v1 in.bin out.bin   # blob hex on stderr
eitb/eitb decrypt singlemsg-triple-mac-v1 <blob-hex> out.bin back.bin
```

## Limitations

- The binding wraps the Triple Pipeline surface only. The Low-Level
  seed / MAC / blob / wrapper / parallax APIs are not exposed — use
  the shipped Go core for those.
- Streaming-decrypt caveat: chunked Streaming AEAD verifies per
  chunk, so plaintext of verified chunks is released before a later
  chunk can fail authentication.
- `ITB_LastError` is process-global last-write-wins; the textual
  diagnostic attached to an `ItbError` may belong to a different
  call under concurrent use. The status code is always attributable.
- `rekey` must not run concurrently with cipher calls or open stream
  sessions on the same `Pipeline`.
- The `transform` / `toLazyList` adapters are single-pass: traverse
  the returned iterator at most once and do not interleave it with
  direct `write` / `read` calls on the same session.
- The Java binding (and through it the JNI shim + libitb3) must be
  built and reachable per the lookup order above.

## License

Apache-2.0 — see [LICENSE](https://github.com/everanium/itb/blob/main/LICENSE).
