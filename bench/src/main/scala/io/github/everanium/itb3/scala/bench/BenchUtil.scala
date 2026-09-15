// Shared timing + reporting helpers for the Scala binding
// micro-benchmarks. Wall-clock via System.nanoTime; output is a
// fixed-width table:
//
//   bench             size     mb_per_sec
//   message           1 MiB    <n>
//   ...
//
// Bench configuration is driven by environment variables so a
// side-by-side comparison with the root Go bench harness is
// straightforward:
//
//   ITB_NONCE_BITS     nonce width (default 512)
//   ITB_KEY_BITS       key bits (default 1024)
//   ITB_WITH_PARALLAX  parallax layer on/off (default false)
//   ITB_WITH_WRAPPER   wrapper layer on/off (default false)
//   ITB_INNER_HASH     opaque hash name (default: profile's)
//   ITB_PROFILE        profile name override
//   ITB_BENCH_MIN_SEC  per-case wall-clock budget (default 5.0)

package io.github.everanium.itb3.scala.bench

import io.github.everanium.itb3.scala.Opts
object BenchUtil:

  /** Iteration floor per case. */
  private val MinIters = 3

  /** Payload sizes exercised by both shapes. */
  val Sizes: Seq[Int] = Seq(1 << 20, 16 << 20, 64 << 20)

  private def env(name: String): Option[String] =
    Option(System.getenv(name)).filter(_.nonEmpty)

  def minSeconds: Double =
    env("ITB_BENCH_MIN_SEC").flatMap(_.toDoubleOption).filter(_ > 0.0).getOrElse(5.0)

  /** Reads the bench-shape env vars and builds an [[Opts]]. Defaults
    * match root Go BENCH3.md so numbers are directly comparable.
    */
  def buildOpts: Opts =
    val base = Opts.empty
      .withNonceBits(envLong("ITB_NONCE_BITS", 512))
      .withKeyBits(envLong("ITB_KEY_BITS", 1024))
      .withParallax(envBool("ITB_WITH_PARALLAX"))
      .withWrapper(envBool("ITB_WITH_WRAPPER"))
    val hashed = env("ITB_INNER_HASH").fold(base)(base.withInnerHash)
    env("ITB_MAC_NAME").fold(hashed)(hashed.withMacName)

  def profileName(fallback: String): String =
    env("ITB_PROFILE").getOrElse(fallback)

  def header(): Unit =
    println(f"${"bench"}%-17s ${"size"}%-8s mb_per_sec")

  private def sizeLabel(size: Int): String =
    if size >= (1 << 20) then s"${size >> 20} MiB" else s"${size >> 10} KiB"

  /** Runs `run` until the wall-clock budget is spent (with an
    * iteration floor + one untimed warm-up), then prints one table
    * row.
    */
  def benchCase(name: String, size: Int)(run: => Unit): Unit =
    run // warm-up
    val budget = minSeconds
    val start = System.nanoTime()
    var iters = 0L
    while (System.nanoTime() - start) / 1e9 < budget || iters < MinIters do
      run
      iters += 1
    val elapsed = (System.nanoTime() - start) / 1e9
    val mb = size.toDouble * iters / (1024.0 * 1024.0)
    println(f"$name%-17s ${sizeLabel(size)}%-8s ${mb / elapsed}%.1f")

  /** CSPRNG-filled payload so plaintext content matches the root Go
    * bench (crypto/rand). Never inside the timing loop.
    */
  def payload(n: Int): Array[Byte] =
    val buf = new Array[Byte](n)
    new java.security.SecureRandom().nextBytes(buf)
    buf

  private def envLong(name: String, fallback: Long): Long =
    env(name).flatMap(_.toLongOption).getOrElse(fallback)

  private def envBool(name: String): Boolean =
    env(name).exists(v => v == "true" || v == "1")
