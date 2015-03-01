import AssemblyKeys._

assemblySettings

// Default values
val defaultScalaVersion     = "2.10.4"
val defaultSparkVersion     = "1.2.0"
val defaultSparkCoreVersion = "2.10"
val defaultSparkHome        = "target"
val defaultHadoopVersion    = "2.4.0"

// Values
val _scalaVersion     = scala.util.Properties.envOrElse("SCALA_VERSION", defaultScalaVersion)
val _sparkVersion     = scala.util.Properties.envOrElse("SPARK_VERSION", defaultSparkVersion)
val _sparkCoreVersion = scala.util.Properties.envOrElse("SPARK_CORE_VERSION", defaultSparkCoreVersion)
val _sparkHome        = scala.util.Properties.envOrElse("SPARK_HOME", defaultSparkHome)
val _hadoopVersion    = scala.util.Properties.envOrElse("HADOOP_VERSION", defaultHadoopVersion)

// Project settings
name := "ruby-spark"

version := "1.0.0"

scalaVersion := _scalaVersion

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

// Jar target folder
artifactPath in Compile in packageBin := file(s"${_sparkHome}/ruby-spark.jar")
outputPath in packageDependency := file(s"${_sparkHome}/ruby-spark-deps.jar")

// Protocol buffer support
seq(sbtprotobuf.ProtobufPlugin.protobufSettings: _*)

// Additional libraries
libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"    % _sparkVersion,
  "org.apache.spark"  %% "spark-graphx"  % _sparkVersion,
  "org.apache.spark"  %% "spark-mllib"   % _sparkVersion,
  "org.apache.hadoop" %  "hadoop-client" % _hadoopVersion
)

// Repositories
resolvers ++= Seq(
  "JBoss Repository"     at "http://repository.jboss.org/nexus/content/repositories/releases/",
  "Spray Repository"     at "http://repo.spray.cc/",
  "Cloudera Repository"  at "https://repository.cloudera.com/artifactory/cloudera-repos/",
  "Akka Repository"      at "http://repo.akka.io/releases/",
  "Twitter4J Repository" at "http://twitter4j.org/maven2/",
  "Apache HBase"         at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven Repo"   at "http://maven.twttr.com/",
  "scala-tools"          at "https://oss.sonatype.org/content/groups/scala-tools",
  "Typesafe repository"  at "http://repo.typesafe.com/typesafe/releases/",
  "Second Typesafe repo" at "http://repo.typesafe.com/typesafe/maven-releases/",
  "Mesosphere Public Repository" at "http://downloads.mesosphere.io/maven",
  Resolver.sonatypeRepo("public")
)

// Merge strategy
mergeStrategy in assembly <<= (mergeStrategy in assembly) { (old) =>
  {
    case m if m.toLowerCase.endsWith("manifest.mf") => MergeStrategy.discard
    case m if m.startsWith("META-INF") => MergeStrategy.discard
    case PathList("javax", "servlet", xs @ _*) => MergeStrategy.first
    case PathList("org", "apache", xs @ _*) => MergeStrategy.first
    case PathList("org", "jboss", xs @ _*) => MergeStrategy.first
    case "about.html"  => MergeStrategy.rename
    case "reference.conf" => MergeStrategy.concat
    case _ => MergeStrategy.first
  }
}