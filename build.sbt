import sbt.Keys.{name, _}


lazy val commonSettings = Seq(
  scalaVersion := "2.12.3",
  scalacOptions += "-deprecation",
  scalacOptions += "-unchecked",
  scalacOptions += "-feature",
  resolvers += Resolver.jcenterRepo,

  organization := "com.avast",
  name := "dapper",
  version := sys.env.getOrElse("TRAVIS_TAG", "0.1-SNAPSHOT"),
  description := "Library for creating DAO instances for case-classes on top of the standard Java Datastax driver",

  licenses ++= Seq("Apache-2.0" -> url(s"https://github.com/avast/${name.value}/blob/${version.value}/LICENSE")),
  publishArtifact in Test := false,
  bintrayOrganization := Some("avast"),
  pomExtra := (
    <scm>
      <url>git@github.com:avast/
        {name.value}
        .git</url>
      <connection>scm:git:git@github.com:avast/
        {name.value}
        .git</connection>
    </scm>
      <developers>
        <developer>
          <id>avast</id>
          <name>Jan Kolena, Avast Software s.r.o.</name>
          <url>https://www.avast.com</url>
        </developer>
      </developers>
    )
)

lazy val coreSettings = Seq(
  libraryDependencies ++= Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",

    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",

    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,

    "org.apache.commons" % "commons-lang3" % "3.6" % "test",
    "com.typesafe" % "config" % "1.3.2" % "test",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",

    "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
)

lazy val root = Project(id = "rootProject",
  base = file(".")) settings (publish := {}) aggregate core

lazy val core = Project(
  id = "core",
  base = file("./core"),
  settings = commonSettings ++ coreSettings ++ Seq(
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
    name := "dapper-core"
  )
)