name    := "spark-iceberg-job"
version := "0.1.0"

scalaVersion := "2.13.16"

val sparkVersion   = "4.0.0"
val icebergVersion = "1.10.1"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-sql"                        % sparkVersion,
  "org.apache.iceberg" % "iceberg-spark-runtime-4.0_2.13"  % icebergVersion,
  "org.apache.hadoop"  % "hadoop-aws"                       % "3.4.1"
)

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", "services", xs @ _*) => MergeStrategy.concat
  case PathList("META-INF", _*)                  => MergeStrategy.discard
  case "reference.conf"                          => MergeStrategy.concat
  case _                                         => MergeStrategy.first
}

// Allow forked JVM for local sbt run
run / fork := true
run / javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.nio=ALL-UNNAMED",
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
)
