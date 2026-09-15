// Bench entry: `message`, `stream`, or `all` (default).

package io.github.everanium.itb3.scala.bench

import io.github.everanium.itb3.scala.Runtime
object Main:

  def main(args: Array[String]): Unit =
    // Go-runtime pacing caps for bench-scale allocation churn; the
    // run_bench.sh env defaults apply the same values at load time —
    // the programmatic setter wins when both are present.
    val _ = Runtime.setMemoryLimit(4L * 1024 * 1024 * 1024)
    val _ = Runtime.setGCPercent(100)
    args.headOption.getOrElse("all") match
      case "message"         => BenchMessage.run()
      case "stream"          => BenchStream.run()
      case "stream_one_shot" => BenchStreamOneShot.run()
      case "all" =>
        BenchMessage.run()
        BenchStream.run()
        BenchStreamOneShot.run()
      case other =>
        System.err.println(
          s"usage: bench [message|stream|stream_one_shot|all] (got: $other)"
        )
        sys.exit(2)
