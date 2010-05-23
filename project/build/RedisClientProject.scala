import sbt._
import sbt.CompileOrder._

class RedisClientProject(info: ProjectInfo) extends DefaultProject(info) 
{
  // override def useDefaultConfigurations = true
  override def compileOptions = super.compileOptions ++
    Seq("-deprecation", "-Xcheckinit", "-Xwarninit", "-encoding", "utf8").map(x => CompileOption(x))


  val scalaToolsSnapshots = "Scala-Tools Maven2 Snapshots Repository" at "http://scala-tools.org/repo-snapshots"
  val scalaToolsReleases = "Scala-Tools Maven2 Releases Repository" at "http://scala-tools.org/repo-releases"
  val scalatest =
    buildScalaVersion match {
      case "2.7.7" => 
        "org.scalatest" % "scalatest" % "1.0" 
      case "2.8.0.Beta1" =>
        "org.scalatest" % "scalatest" % "1.0.1-for-scala-2.8.0.Beta1-with-test-interfaces-0.3-SNAPSHOT" 
      case "2.8.0.RC2" =>
        "org.scalatest" % "scalatest" % "1.2-for-scala-2.8.0.RC2-SNAPSHOT" % "test"
    }
  val junit = "junit" % "junit" % "4.8.1"
}
