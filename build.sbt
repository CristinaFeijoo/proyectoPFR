ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "3.1.0"

lazy val root = (project in file("."))
  .settings(
    name := "ProyectoIntegra",
    idePackagePrefix := Some("ec.edu.utpl.computacion.pfr.pi"),
    libraryDependencies ++= Seq(
      "com.github.tototoshi" %% "scala-csv" % "1.3.10",
      "io.github.pityka" %% "nspl-awt" % "0.10.0",
      "io.github.pityka" %% "nspl-core" % "0.10.0",
      "io.github.pityka" %% "nspl-saddle" % "0.10.0",
      "org.tpolecat" %% "doobie-core" % "1.0.0-M5",
      "org.tpolecat" %% "doobie-hikari" % "1.0.0-M5",
      "mysql" % "mysql-connector-java" % "8.0.23",
      "org.scalaj" %% "scalaplot" % "0.4.1"    )
  )
