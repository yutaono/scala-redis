import sbt._

class RedisClientProject(info: ProjectInfo) extends DefaultProject(info) 
{
  override def useDefaultConfigurations = true

  val scalaToolsSnapshots = "Scala-Tools Maven2 Releases Repository" at "http://scala-tools.org/repo-releases"
  val scalatest = "org.scalatest" % "scalatest" % "1.0" 
  val junit = "junit" % "junit" % "4.5"
}
