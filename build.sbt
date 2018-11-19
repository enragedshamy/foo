name := "SimilarityAnalysisAufgabe"
version := "0.1"

scalaVersion := "2.11.11"

parallelExecution in Test := false

libraryDependencies ++=Seq("org.apache.spark" %% "spark-core" % "2.2.0",
			   "org.apache.spark" %% "spark-sql" % "2.2.0",
			   "org.jfree" % "jfreechart" % "1.0.19",
			   "junit" % "junit" % "4.12",
			   "org.scalactic" %% "scalactic" % "3.0.5",
			   "org.scalatest" %% "scalatest" % "3.0.5" % "test")



