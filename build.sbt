name := "Test1"

version := "0.1"

scalaVersion := "2.12.7"

dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-core" % "2.9.7"
dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.9.7"
dependencyOverrides += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.9.7"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "2.4.0",
  "org.apache.spark" %% "spark-graphx" % "2.4.0",
  "org.apache.spark" %% "spark-sql" % "2.4.0",
  "org.apache.spark" %% "spark-streaming" % "2.4.0" % "provided",
  "com.orientechnologies" % "orientdb-core" % "3.0.11",
  "com.orientechnologies" % "orientdb-graphdb" % "3.0.11",
  "com.orientechnologies" % "orientdb-object" % "3.0.11",
  "com.orientechnologies" % "orientdb-client" % "3.0.11",
  "com.orientechnologies" % "orientdb-server" % "3.0.11",
  "com.orientechnologies" % "orientdb-parent" % "3.0.11",
  "com.orientechnologies" % "orientdb-tools" % "3.0.11",
  "com.orientechnologies" % "orientdb-distributed" % "3.0.11",
  "com.orientechnologies" % "orientdb-studio" % "3.0.11",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.1.1",
  "com.tinkerpop.blueprints" % "blueprints-core" % "2.6.0",
  "com.tinkerpop.blueprints" % "blueprints-orient-graph" % "2.4.0" % "provided"
)
