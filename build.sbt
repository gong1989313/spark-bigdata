val sparkCore = "org.apache.spark" % "spark-core_2.11" % "2.2.0" % "provided" withSources()
val sparkSql = "org.apache.spark" % "spark-sql_2.11" % "2.2.0" % "provided" withSources()
val sparkStreaming = "org.apache.spark" % "spark-streaming_2.11" % "2.2.0" % "provided" withSources()

val mssqlJdbc = "com.microsoft.sqlserver" % "mssql-jdbc" % "6.2.1.jre8" 
val commonsDbutils = "commons-dbutils" % "commons-dbutils" % "1.7"
val commonsPool = "commons-pool" % "commons-pool" % "1.6"
val commonsDbcp = "commons-dbcp" % "commons-dbcp" % "1.4"
val scalikeJdbc = "org.scalikejdbc" % "scalikejdbc_2.11" % "3.0.2"
val scalikejdbcConfig = "org.scalikejdbc" % "scalikejdbc-config_2.11" % "3.0.2"
val logbackClassic = "ch.qos.logback" % "logback-classic" % "1.2.3"
val config = "com.typesafe" % "config" % "1.3.1"
val scalaTest = "org.scalatest" % "scalatest_2.11" % "3.0.3" % "test"
val playJson = "com.typesafe.play" % "play-json_2.11" % "2.6.2"
val spoiwo = "com.norbitltd" % "spoiwo_2.11" % "1.2.0"
val jxl = "net.sourceforge.jexcelapi" % "jxl" % "2.6.12"
val storm = "org.apache.storm" % "storm-core" % "1.1.1" % "provided"
val mongodb = "org.mongodb.spark" % "mongo-spark-connector_2.11" % "2.2.0"
val mysqlJdbc = "mysql" % "mysql-connector-java" % "8.0.7-dmr"

lazy val commonSettings = Seq(
	organization := "com.gxq.learn",
	version := "0.1",
	scalaVersion := "2.11.8",
	resolvers += "Sonatype OSS Snapshots" at "http://maven.aliyun.com/nexus/content/groups/public/",
	assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false),
	assemblyMergeStrategy in assembly := {
		case PathList("META-INF", xs @ _*) =>
		(xs map{_.toLowerCase}) match {
			case ("manifest.mf" :: Nil) => MergeStrategy.discard
			case _ => MergeStrategy.discard
		}
		case x => MergeStrategy.first
	},
	externalResolvers := Resolver.withDefaultResolvers(resolvers.value, mavenCentral = false)
)

lazy val bigdata = (project in file("."))
	.aggregate(gpfDWReconTool)
	.settings(
		commonSettings,
		name := "bigdata",
		version := "0.0.1"
	)
		
lazy val gpfDWReconTool = (project in file("gpfDWReconTool"))
	.settings(
		commonSettings,
		name := "gpfDWReconTool",
		version :="0.0.1",
		assemblyJarName in assembly := "gpfDWReconTool.jar",
		test in assembly :={},
		dependencyOverrides += "com.fasterxml.jackson.core" % "jackson-databind" % "2.8.9",
		dependencyOverrides += "com.fasterxml.jackson.module" % "jackson-module-scala_2.11" % "2.8.9",
		libraryDependencies ++= Seq(
			sparkCore,
			sparkSql,
			sparkStreaming,
			mssqlJdbc,
			commonsDbutils,
			commonsPool,
			commonsDbcp,
			scalikeJdbc,
			scalikejdbcConfig,
			logbackClassic,
			config,
			scalaTest,
			playJson,
			spoiwo,
			jxl,
			mongodb,
			mysqlJdbc
		),
		libraryDependencies ~= { _.map(_.exclude("org.slf4j", "slf4j-log4j12")) }
	)



