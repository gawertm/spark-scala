import mill._, scalalib._

object MillScalaSeed extends ScalaModule {
  def scalaVersion = "2.13.7"

  object test extends Tests {
    def ivyDeps = Agg(ivy"org.scalameta::munit:0.7.29")
    def testFrameworks = Seq("munit.Framework")
  }
}