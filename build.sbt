ThisBuild / version := "0.1.0-SNAPSHOT"

val sparkProfile = sys.props.getOrElse("spark.profile", "3.5")

val sparkVersion = sparkProfile match {
  case "3.5" => "3.5.0"
  case "4.0" => "4.0.0"
  case other => sys.error(s"Unsupported spark.profile '$other'. Use 3.5 or 4.0.")
}

val scalaVersionForSpark = sparkProfile match {
  case "3.5" => "2.12.18"
  case "4.0" => "2.13.16"
}

val deltaVersion = sparkProfile match {
  case "3.5" => "3.2.0"
  case "4.0" => "4.0.0"
}

ThisBuild / scalaVersion := scalaVersionForSpark
ThisBuild / crossScalaVersions := Seq("2.12.18", "2.13.16")

lazy val root = (project in file("."))
  .settings(
    name := "learningspark",
    idePackagePrefix := Some("learningSpark"),
    Test / fork := true,
    Test / parallelExecution := false,
    Test / classLoaderLayeringStrategy := ClassLoaderLayeringStrategy.Flat,
    Test / javaOptions ++= Seq(
      "--add-exports=java.base/sun.nio.ch=ALL-UNNAMED"
    ),
    libraryDependencies ++= Seq(
      // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
      "org.apache.spark" %% "spark-sql" % sparkVersion,
      // https://mvnrepository.com/artifact/org.apache.spark/spark-core
      "org.apache.spark" %% "spark-core" % sparkVersion,
      // File path stuff
      // https://mvnrepository.com/artifact/com.lihaoyi/os-lib
      "com.lihaoyi" %% "os-lib" % "0.7.1",
      // Please note that the Delta Lake on Spark Maven artifact has been
      // renamed from delta-core (before 3.0) to delta-spark (3.0 and above).
      // https://mvnrepository.com/artifact/io.delta/delta-spark
      "io.delta" %% "delta-spark" % deltaVersion,
      // For Chapter 10!
      // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
      "org.apache.spark" %% "spark-mllib" % sparkVersion,
      // For Chapter 11!
      // https://mvnrepository.com/artifact/org.mlflow/mlflow-client
      "org.mlflow" % "mlflow-client" % "2.15.0",
      // For Chapter 11 - Xgboost
      // https://mvnrepository.com/artifact/ml.dmlc/xgboost4j-spark
      "ml.dmlc" %% "xgboost4j-spark" % "2.1.0",
      // for testing
      "org.scalatest" %% "scalatest" % "3.2.18" % Test
    )
    // if you want to change the name of your jar
    // assembly / assemblyJarName := "SparkBasics.jar",
    // After packaging, you can set the main class with:
    // Compile/mainClass := Some("sezai.Main")
    // set main class for assembly jar
    // This is needed if you are using the assembly plugin.
    // What is sbt assembly ? - https://github.com/sbt/sbt-assembly
    // assembly / mainClass := Some("sezai.Main")
    // exclude Scala library from assembly
    // assembly / assemblyOption  := (assembly / assemblyOption).value.copy(includeScala = false)
    // merging strategy to avoid errors at
    // sbt assembly
    // assembly / assemblyMergeStrategy := {
    //   case PathList("META-INF", _*) => MergeStrategy.discard
    //   case _                        => MergeStrategy.first
    // }
  )
