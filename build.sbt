// ITB Scala binding — thin idiomatic layer over the Java binding
// (bindings/java). The Java jar is consumed as an unmanaged jar so
// the monorepo build needs no local Maven publish step.

// ITB_JAVA_LIBS_DIR overrides the jar location (e.g. an installed
// copy of the Java binding); the default is the monorepo sibling.
val javaLibsDir = Def.setting {
  sys.env.get("ITB_JAVA_LIBS_DIR") match {
    case Some(dir) => file(dir)
    case None      => (ThisBuild / baseDirectory).value / ".." / "java" / "build" / "libs"
  }
}

// The library jar only — the eitb.jar / bench.jar tool jars embed
// duplicate copies of the library classes.
val javaBindingJars = Def.setting {
  (javaLibsDir.value * "libitb3-java-*.jar").classpath
}

lazy val commonSettings = Seq(
  organization := "io.github.everanium",
  version := "0.5.1",
  description := "ITB Symmetric Cipher Construction with Ambiguity-Based Security - Scala",
  homepage := Some(url("https://github.com/everanium/itb")),
  licenses := Seq(
    "Apache-2.0" -> url("https://www.apache.org/licenses/LICENSE-2.0")
  ),
  developers := List(
    Developer(
      id = "everanium",
      name = "Andrey Kuvshinov",
      email = "andrew@encloud.blue",
      url = url("https://github.com/everanium")
    )
  ),
  scmInfo := Some(
    ScmInfo(
      url("https://github.com/everanium/itb"),
      "scm:git:https://github.com/everanium/itb.git",
      Some("scm:git:ssh://git@github.com/everanium/itb.git")
    )
  ),
  // sbt has no issueManagement key; the POM element is injected directly.
  pomExtra :=
    <issueManagement>
      <system>GitHub Issues</system>
      <url>https://github.com/everanium/itb/issues</url>
    </issueManagement>,
  scalaVersion := "3.6.2",
  scalacOptions ++= Seq(
    "-deprecation",
    "-feature",
    "-unchecked",
    "-Werror"
  ),
  Compile / unmanagedJars ++= javaBindingJars.value,
  // The Java binding loads the JNI shim in-process; fork so the
  // ITB_JNI_PATH env var and JVM flags reach the running code
  // instead of the sbt shell JVM.
  run / fork := true,
  Test / fork := true,
  outputStrategy := Some(StdoutOutput)
)

lazy val root = (project in file("."))
  .settings(commonSettings)
  .settings(
    name := "libitb3-scala",
    libraryDependencies += "org.scalameta" %% "munit" % "1.0.4" % Test
  )

lazy val bench = (project in file("bench"))
  .dependsOn(root)
  .settings(commonSettings)
  .settings(name := "libitb3-scala-bench")

lazy val eitb = (project in file("eitb"))
  .dependsOn(root)
  .settings(commonSettings)
  .settings(name := "libitb3-scala-eitb")

lazy val loop = (project in file("loop"))
  .dependsOn(root)
  .settings(commonSettings)
  .settings(name := "libitb3-scala-loop")
