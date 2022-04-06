package demo

import cats.effect.IO
import doobie.implicits._
import doobie.util.transactor.Transactor

// The YOLO Mode
object YoloApp extends App {

  import cats.effect.unsafe.implicits.global

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
  query.quick.unsafeRunSync()
}
