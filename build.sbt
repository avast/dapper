import sbt.Keys.{name, _}

enablePlugins(spray.boilerplate.BoilerplatePlugin)

lazy val commonSettings = Seq(
  scalaVersion := "2.12.3",
  scalacOptions += "-deprecation",
  scalacOptions += "-unchecked",
  scalacOptions += "-feature",
  resolvers += Resolver.jcenterRepo,
  resolvers +=
    "Avast repo" at "https://artifactory.int.avast.com/artifactory/maven",

  organization := "com.avast",
  name := "dapper",
  version := sys.env.getOrElse("TRAVIS_TAG", "0.1-SNAPSHOT"),
  description := "Library for mapping case classes to Datastax statements",

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

lazy val macroSettings = Seq(
  libraryDependencies ++= Seq(
    "org.scala-lang" % "scala-reflect" % scalaVersion.value,
    "org.scala-lang" % "scala-compiler" % scalaVersion.value,
    "org.scalactic" %% "scalactic" % "3.0.0",
    "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
)

lazy val coreSettings = Seq(
  libraryDependencies ++= Seq(
    "com.datastax.cassandra" % "cassandra-driver-core" % "3.3.0",
    "com.datastax.cassandra" % "cassandra-driver-extras" % "3.3.0",
    "com.datastax.cassandra" % "cassandra-driver-mapping" % "3.3.0",

    "com.typesafe.scala-logging" %% "scala-logging" % "3.7.2",

    "com.avast.utils2" %% "utils-datastax" % "6.2.68" % "test",
    "org.apache.commons" % "commons-lang3" % "3.6" % "test",
    "ch.qos.logback" % "logback-classic" % "1.2.3" % "test",

    "org.scalatest" %% "scalatest" % "3.0.0" % "test"
  )
)

lazy val root = Project(id = "rootProject",
  base = file(".")) settings (publish := {}) aggregate(macros, core)

lazy val macros = Project(
  id = "macros",
  base = file("./macros"),
  settings = commonSettings ++ macroSettings ++ Seq(
    addCompilerPlugin("org.scalamacros" % "paradise" % "2.1.1" cross CrossVersion.full),
    name := "dapper-macros"
  )
)

lazy val core = Project(
  id = "core",
  base = file("./core"),

  settings = commonSettings ++ coreSettings ++ Seq(
    name := "dapper-core"
  )
).dependsOn(macros)