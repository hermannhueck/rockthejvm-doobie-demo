package demo

import hutil.stringformat._
import cats.effect._

object DoobieDemo extends IOApp.Simple {

  val run: IO[Unit] =
    for {
      _ <- IO.println(dash80.green)
      _ <- IO.println("Hello, world!")
      _ <- IO.println(dash80.green)
    } yield ()
}
