// eitb — command-line demonstrator for the ITB Scala binding.
//
// Subcommands:
//
//   eitb version                                   library + binding versions
//   eitb profiles                                  registered profile catalogue
//   eitb encrypt <profile> <in-file> <out-file>    Single Message encrypt
//   eitb decrypt <profile> <blob-hex> <in-file> <out-file>
//
// `encrypt` prints the session blob to stderr as hex; feed that hex
// back to `decrypt` on the receiving side. `profiles` lists the
// registered profile catalogue one name per line; the profiles that
// carry a cipher surface are the ones `encrypt` / `decrypt` accept.

package io.github.everanium.itb3.scala.eitb

import java.nio.file.{Files, Paths}

import scala.util.Using

import io.github.everanium.itb3.scala.{ItbError, Pipeline, Runtime}
object Main:

  def main(args: Array[String]): Unit =
    val _ = Runtime.setMemoryLimit(4L * 1024 * 1024 * 1024)
    val _ = Runtime.setGCPercent(100)
    val rc =
      try
        args.toList match
          case "version" :: Nil                              => cmdVersion()
          case "profiles" :: Nil                             => cmdProfiles()
          case "encrypt" :: profile :: in :: out :: Nil      => cmdEncrypt(profile, in, out)
          case "decrypt" :: profile :: blob :: in :: out :: Nil =>
            cmdDecrypt(profile, blob, in, out)
          case _ =>
            System.err.println(
              """usage: eitb version
                |       eitb profiles
                |       eitb encrypt <profile> <in-file> <out-file>
                |       eitb decrypt <profile> <blob-hex> <in-file> <out-file>""".stripMargin
            )
            2
      catch
        case e: ItbError =>
          System.err.println(s"eitb: ${e.getMessage}")
          1
        case e: Exception =>
          System.err.println(s"eitb: ${e.getMessage}")
          1
    sys.exit(rc)

  private def orThrow[A](result: Either[ItbError, A]): A =
    result.fold(e => throw e, identity)

  private def cmdVersion(): Int =
    println(s"libitb3 ${Runtime.version}")
    println(s"itb-scala ${Runtime.BindingVersion}")
    0

  // Prints the registered profile catalogue one name per line in the
  // sorted order Pipeline.profiles() returns.
  private def cmdProfiles(): Int =
    orThrow(Pipeline.profiles()).foreach(println)
    0

  // Profiles whose canonical name begins with "streaming-" route
  // through the one-shot streaming buffered pair instead of the
  // Single Message pair.
  private def isStreamingProfile(profile: String): Boolean =
    profile.startsWith("streaming-")

  // Recursively create the parent directory of `path` (mkdir -p).
  private def ensureParentDir(path: String): Unit =
    val parent = Paths.get(path).toAbsolutePath.getParent
    if parent != null then
      val _ = Files.createDirectories(parent)

  private def cmdEncrypt(profile: String, inFile: String, outFile: String): Int =
    val plain = Files.readAllBytes(Paths.get(inFile))
    Using.resource(orThrow(Pipeline.init(profile))) { pipe =>
      val wire = orThrow(
        if isStreamingProfile(profile) then pipe.encryptStreamOneShot(plain)
        else pipe.encryptMessage(plain)
      )
      ensureParentDir(outFile)
      Files.write(Paths.get(outFile), wire)
      System.err.println(hex(orThrow(pipe.save())))
      println(s"encrypted $inFile -> $outFile (${plain.length} -> ${wire.length} bytes)")
    }
    0

  private def cmdDecrypt(profile: String, blobHex: String, inFile: String, outFile: String): Int =
    val blob = unhex(blobHex)
    val wire = Files.readAllBytes(Paths.get(inFile))
    Using.resource(orThrow(Pipeline.load(blob))) { pipe =>
      val plain = orThrow(
        if isStreamingProfile(profile) then pipe.decryptStreamOneShot(wire)
        else pipe.decryptMessage(wire)
      )
      ensureParentDir(outFile)
      Files.write(Paths.get(outFile), plain)
      println(s"decrypted $inFile -> $outFile (${wire.length} -> ${plain.length} bytes)")
    }
    0

  private def hex(bytes: Array[Byte]): String =
    val sb = new StringBuilder(bytes.length * 2)
    bytes.foreach(b => sb.append(f"${b & 0xff}%02x"))
    sb.toString

  private def unhex(s: String): Array[Byte] =
    require(s.length % 2 == 0, "odd-length hex string")
    Array.tabulate(s.length / 2)(i => Integer.parseInt(s.substring(2 * i, 2 * i + 2), 16).toByte)
