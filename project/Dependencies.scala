import sbt._

object Dependencies {

  import Versions._

  lazy val doobieCore     = "org.tpolecat"   %% "doobie-core"     % doobieVersion
  lazy val doobiePostgres = "org.tpolecat"   %% "doobie-postgres" % doobieVersion
  lazy val doobieHikari   = "org.tpolecat"   %% "doobie-hikari"   % doobieVersion
  lazy val newtype        = "io.estatico"    %% "newtype"         % newTypeVersion
  lazy val munit          = "org.scalameta"  %% "munit"           % munitVersion
  lazy val scalaCheck     = "org.scalacheck" %% "scalacheck"      % scalaCheckVersion

  // https://github.com/typelevel/kind-projector
  lazy val kindProjectorPlugin    = compilerPlugin(
    compilerPlugin("org.typelevel" % "kind-projector" % kindProjectorVersion cross CrossVersion.full)
  )
  // https://github.com/oleg-py/better-monadic-for
  lazy val betterMonadicForPlugin = compilerPlugin(
    compilerPlugin("com.olegpy" %% "better-monadic-for" % betterMonadicForVersion)
  )
}
