package demo

import cats.effect.{IO, IOApp}
import doobie.implicits._
import doobie.util.transactor.Transactor
import hutil.stringformat._

// The YOLO Mode
object YoloIOApp extends IOApp.Simple {

  val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql:myimdb",
    "docker",
    "docker"
  )

  val y = xa.yolo
  import y._

  // Remember, You Only Load Once!

  val query = sql"select name from actors".query[String].to[List]

  val run: IO[Unit] =
    for {
      _ <- IO.println(dash80.green)
      _ <- query.quick
      _ <- IO.println(dash80.green)
    } yield ()
}
