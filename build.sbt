ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "learningspark",
    idePackagePrefix := Some("learningSpark"),
    // if you want to change the name of your jar
    // assembly / assemblyJarName := "SparkBasics.jar",
    // After packaging, you can set the main class with:
    // Compile/mainClass := Some("sezai.Main")
  )

val sparkVersion = "3.5.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion

// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion

// File path stuff
// https://mvnrepository.com/artifact/com.lihaoyi/os-lib
libraryDependencies += "com.lihaoyi" %% "os-lib" % "0.7.1"

// For DeltaLake - (this is old)
// https://mvnrepository.com/artifact/io.delta/delta-core
// libraryDependencies += "io.delta" %% "delta-core" % "3.1.0"

// Please note that the Delta Lake on Spark Maven artifact has been
// renamed from delta-core (before 3.0) to delta-spark (3.0 and above).
// https://mvnrepository.com/artifact/io.delta/delta-spark
libraryDependencies += "io.delta" %% "delta-spark" % "3.2.0"

// For Chapter 10!
// https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0"

// For Chapter 11!
// https://mvnrepository.com/artifact/org.mlflow/mlflow-client
libraryDependencies += "org.mlflow" % "mlflow-client" % "2.15.0"

// set main class for assembly jar
// This is needed if you are using the assembly plugin.
// What is sbt assembly ? - https://github.com/sbt/sbt-assembly
// assembly / mainClass := Some("sezai.Main")

// for testing
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.18" % "test"

// exclude Scala library from assembly
// assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)

// merging strategy to avoid errors at
// sbt assembly
// assembly / assemblyMergeStrategy := {
//   case PathList("META-INF", _*) => MergeStrategy.discard
//   case _                        => MergeStrategy.first
// }