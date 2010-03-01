import sbt._

class RedisClientProject(info: ProjectInfo) extends DefaultProject(info) 
{
  override def useDefaultConfigurations = true

  val scalaToolsSnapshots = "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"
  val scalaToolsReleases = "Scala-Tools Maven2 Releases Repository" at "http://scala-tools.org/repo-releases"
  val scalatest = "org.scalatest" % "scalatest" % "1.0.1-for-scala-2.8.0.Beta1-with-test-interfaces-0.3-SNAPSHOT" 
  val junit = "junit" % "junit" % "4.8.1"
}
