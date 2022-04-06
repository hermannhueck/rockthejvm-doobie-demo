// See:
// - https://blog.rockthejvm.com/doobie/
// - https://www.youtube.com/watch?v=SvFL7c6F9xI&list=PLmtsMNDRU0ByzHzqLdoaeuKntdwCCB1d3&index=5
// - https://www.youtube.com/watch?v=9xgOQh-Ppao&list=PLmtsMNDRU0ByzHzqLdoaeuKntdwCCB1d3&index=6

package demo

import java.util.UUID

import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import doobie._
import doobie.hikari.HikariTransactor
import doobie.implicits._
import doobie.postgres.implicits._
import doobie.util.transactor.Transactor
import doobie.util.update.Update
import doobie.util.{Get, Put, Read, Write}
import hutil.stringformat._

object DoobieDemo extends IOApp.Simple {

  case class Actor(id: Int, name: String)

  case class Movie(id: String, title: String, year: Int, actors: List[String], director: String)

  implicit final class DebugOps[A](private val io: IO[A]) extends AnyVal {
    @inline def debug: IO[A] =
      io.flatMap(a => IO.println(s"[${Thread.currentThread().getName()}] $a").as(a))
  }

  val xa: Transactor[IO] = Transactor.fromDriverManager[IO](
    "org.postgresql.Driver",
    "jdbc:postgresql:myimdb",
    "docker",
    "docker"
  )

  val postgres: Resource[IO, HikariTransactor[IO]] = for {
    ce <- ExecutionContexts.fixedThreadPool[IO](32)
    xa <- HikariTransactor.newHikariTransactor[IO](
            "org.postgresql.Driver",
            "jdbc:postgresql:myimdb",
            "postgres",
            "example", // The password
            ce
          )
  } yield xa

  def findAllActorsNames0: IO[List[String]] = { // example from blog
    val findAllActorsQuery: doobie.Query0[String]        = sql"select name from actors".query[String]
    // The type ConnectionIO[A] represents a computation that, given a Connection, will generate a value of type IO[A].
    val findAllActors: doobie.ConnectionIO[List[String]] = findAllActorsQuery.to[List]
    findAllActors.transact(xa)
  }

  // Doobie defines all its most essential types as instances of the Free monad.
  // Every free monad is only a description of a program. It’s not executable at all since it requires an interpreter.
  // The interpreter, in this case, is the Transactor we created. Its role is to compile the program
  // into a Kleisli[IO, Connection, A]. The course on Cats explains Kleisli in depth, but in short,
  // the previous Kleisli is another representation of the function Connection => IO[A].

  val findAllActorNames: IO[List[String]] =
    sql"select name from actors"
      .query[String]
      .to[List]
      .transact(xa)

  def findAllActorsIdsAndNames: IO[List[(Int, String)]] =
    sql"select id, name from actors"
      .query[(Int, String)]
      .to[List]
      .transact(xa)

  val findAllActors: IO[List[Actor]] =
    sql"select id, name from actors"
      .query[Actor]
      .to[List]
      .transact(xa)

  def findActorById(id: Int): IO[Actor] =
    sql"select id, name from actors where id = $id"
      .query[Actor]
      .unique
      .transact(xa)

  def findMaybeActorById(id: Int): IO[Option[Actor]] =
    sql"select id, name from actors where id = $id"
      .query[Actor]
      .option
      .transact(xa)

  val findAllActorNamesStream: fs2.Stream[ConnectionIO, String] =
    sql"select name from actors"
      .query[String]
      .stream
  val findAllActorNamesList: IO[List[String]]                   =
    findAllActorNamesStream
      .compile
      .toList
      .transact(xa)

  val findAllActorNamesViaStream: IO[List[String]] =
    sql"select name from actors"
      .query[String]
      .stream
      .compile
      .toList
      .transact(xa)

  // HC, HPS
  def findActorByName(name: String): IO[Option[Actor]] =
    HC.stream[Actor](s"select id, name from actors where name = ?", HPS.set(name), chunkSize = 100)
      .compile
      .toList
      .map(_.headOption)
      .transact(xa)
  val findHenryCavill                                  = findActorByName("Henry Cavill")

  // fragments
  def findActorsByInitial(letter: Char): IO[List[Actor]] = {
    val select: Fragment    = fr"select id, name"
    val from: Fragment      = fr"from actors"
    val where: Fragment     = fr"where LEFT(name, 1) = ${letter.toString}"
    val statement: Fragment = select ++ from ++ where
    statement.query[Actor].to[List].transact(xa)
  }
  val findActorsByInitialH                               = findActorsByInitial('H')

  // fragments are Monoids
  def findActorsByInitialUsingMonoid(letter: Char): IO[List[Actor]] = {
    import cats.syntax.monoid._
    val select: Fragment    = fr"select id, name"
    val from: Fragment      = fr"from actors"
    val where: Fragment     = fr"where LEFT(name, 1) = ${letter.toString}"
    val statement: Fragment = select |+| from |+| where
    statement.query[Actor].to[List].transact(xa)
  }
  val findActorsByInitialUsingMonoidH                               = findActorsByInitialUsingMonoid('H')

  // IN operator supported by fragments
  def findActorsByNames(actorNames: NonEmptyList[String]): IO[List[Actor]] = {
    val sqlStatement: Fragment =
      fr"select id, name from actors where " ++
        Fragments.in(fr"name", actorNames) // name IN (...)
    sqlStatement
      .query[Actor]
      .stream
      .compile
      .toList
      .transact(xa)
  }

  // insert
  def saveActor0(id: Int, name: String): IO[Int] = {
    val update: Update0                     =
      sql"insert into actors (id, name) values ($id, $name)".update
    val saveActor: doobie.ConnectionIO[Int] = update.run
    saveActor.transact(xa)
  }

  def saveActor(id: Int, name: String): IO[Int] =
    sql"insert into actors (id, name) values ($id, $name)"
      .update
      .run
      .transact(xa)

  def saveActorV2(id: Int, name: String): IO[Int] =
    Update[Actor]("insert into actors (id, name) values (?, ?)")
      .run(Actor(id, name))
      .transact(xa)

  // insert with autogenerated unique id
  def saveActor(name: String): IO[Int] =
    sql"insert into actors (name) values ($name)"
      .update
      .withUniqueGeneratedKeys[Int]("id")
      .transact(xa)
  val saveDanielCraig                  = saveActor("Daniel Craig")

  def saveAndGetActor(name: String): IO[Actor] = {
    val retrievedActor = for {
      id    <- sql"insert into actors (name) values ($name)".update.withUniqueGeneratedKeys[Int]("id")
      actor <- sql"select * from actors where id = $id".query[Actor].unique
    } yield actor
    retrievedActor.transact(xa)
  }
  val saveAndGetJohnTravolta                   = saveAndGetActor("John Travolta")

  // update/insert many
  // returns the number of affected rows
  def saveMultipleActors0(names: List[String]): IO[Int]        =
    Update[String]("insert into actors (name) values (?)")
      .updateMany(names)
      .transact(xa)
  // returns a List of inserted actors
  def saveMultipleActors(names: List[String]): IO[List[Actor]] =
    Update[String]("insert into actors (name) values (?)")
      .updateManyWithGeneratedKeys[Actor]("id", "name")(names)
      .compile
      .toList
      .transact(xa)
  val saveAliceAndBob                                          = saveMultipleActors(List("Alice", "Bob"))

  val updateJLYearOfProduction: IO[Int] = {
    val year = 2021
    val id   = "5e5a39bb-a497-4432-93e8-7322f16ac0b2"
    sql"update movies set year_of_production = $year where id = $id".update.run.transact(xa)
  }

  // type classes GET and PUT
  class ActorName(val value: String) extends AnyVal {
    override def toString(): String = value
  }
  object ActorName {
    def apply(value: String): ActorName       = new ActorName(value)
    implicit val actorNameGet: Get[ActorName] = Get[String].map(ActorName(_))
    implicit val actorNamePut: Put[ActorName] = Put[String].contramap(_.value)
  }

  val findActorNamesCostomClass: IO[List[ActorName]] =
    sql"select name from actors"
      .query[ActorName]
      .to[List]
      .transact(xa)

  // type classes Read and Write
  case class DirectorId(value: Int)          extends AnyVal
  case class DirectorName(value: String)     extends AnyVal
  case class DirectorLastName(value: String) extends AnyVal
  case class Director(id: DirectorId, name: DirectorName, lastName: DirectorLastName)
  object Director {
    // allows reads and writes from/to a database by doobie
    implicit val directorRead: Read[Director]   = Read[(Int, String, String)].map { case (id, name, lastName) =>
      Director(DirectorId(id), DirectorName(name), DirectorLastName(lastName))
    }
    implicit val directorWrite: Write[Director] = Write[(Int, String, String)].contramap {
      case Director(DirectorId(id), DirectorName(name), DirectorLastName(lastName)) => (id, name, lastName)
    }
  }

  // large and complex queries
  def findMovieByTitle(title: String): IO[Option[Movie]] =
    sql"""
      select
        m.id,
        m.title,
        m.year_of_production,
        array_agg(a.name) as actors,
        d.name || ' ' || d.last_name
      from movies m
      join movies_actors ma on m.id = ma.movie_id
      join actors a on ma.actor_id = a.id
      join directors d on m.director_id = d.id
      where m.title = $title
      group by m.id, m.title, m.year_of_production, d.name, d.last_name
    """
      .query[Movie]
      .option
      .transact(xa)
  val findMovieWithTitleZackSnyder                       = findMovieByTitle("Zack Snyder's Justice League")

  def findMovieByTitleV2(title: String): IO[Option[Movie]] = {

    def findMovieByTitle: ConnectionIO[Option[(UUID, String, Int, Int)]] =
      sql"select id, title, year_of_production, director_id from movies where title = $title"
        .query[(UUID, String, Int, Int)]
        .option

    def findDirectorById(directorId: Int): ConnectionIO[Option[(String, String)]] =
      sql"select name, last_name from directors where id = $directorId"
        .query[(String, String)]
        .option

    def findActorsByMovieId(movieId: UUID): ConnectionIO[List[String]] =
      sql"""
        select a.name
        from actors a
        join movies_actors ma on a.id = ma.actor_id
        where ma.movie_id = ${movieId}
        """
        .query[String]
        .to[List]

    val query: ConnectionIO[Option[Movie]] = for {
      maybeMovie: Option[(UUID, String, Int, Int)] <- findMovieByTitle
      maybeDirector: Option[(String, String)]      <- maybeMovie match {
                                                        case Some((_, _, _, directorId)) => findDirectorById(directorId)
                                                        case None                        => Option.empty[(String, String)].pure[ConnectionIO]
                                                      }
      actors: List[String]                         <- maybeMovie match {
                                                        case Some((movieId, _, _, _)) => findActorsByMovieId(movieId)
                                                        case None                     => List.empty[String].pure[ConnectionIO]
                                                      }
    } yield for {
      (id, title, yearOfProduction, _) <- maybeMovie
      (firstName, lastName)            <- maybeDirector
    } yield Movie(id.toString, title, yearOfProduction, actors, s"$firstName $lastName")

    // val query2 = for {
    //   movie: Option[(String, String, Int, Int)] <- findMovieByTitle(title)
    //   director: Option[(String, String)]        <- movie.map(m => findDirector(m._4)).getOrElse(IO.pure(None))
    //   actors                                    <- movie.map(m => findActorsByMovieId(m._1)).getOrElse(IO.pure(List.empty))
    // } yield movie.map(m => m.copy(director = director, actors = actors))

    query.transact(xa)
  }
  val findMovieWithTitleZackSnyderV2 = findMovieByTitleV2("Zack Snyder's Justice League")

  def findMovieByTitleV3(movieName: String): IO[Option[Movie]] = {

    def findMovieByTitle() =
      sql"""
           | select id, title, year_of_production, director_id
           | from movies
           | where title = $movieName"""
        .stripMargin
        .query[(UUID, String, Int, Int)]
        .option

    def findDirectorById(directorId: Int) =
      sql"select name, last_name from directors where id = $directorId"
        .query[(String, String)]
        .to[List]

    def findActorsByMovieId(movieId: UUID) =
      sql"""
           | select a.name
           | from actors a
           | join movies_actors ma on a.id = ma.actor_id
           | where ma.movie_id = $movieId
           |"""
        .stripMargin
        .query[String]
        .to[List]

    val query = for {
      maybeMovie <- findMovieByTitle()
      directors  <- maybeMovie match {
                      case Some((_, _, _, directorId)) => findDirectorById(directorId)
                      case None                        => List.empty[(String, String)].pure[ConnectionIO]
                    }
      actors     <- maybeMovie match {
                      case Some((movieId, _, _, _)) => findActorsByMovieId(movieId)
                      case None                     => List.empty[String].pure[ConnectionIO]
                    }
    } yield {
      maybeMovie.map { case (id, title, year, _) =>
        val directorName     = directors.head._1
        val directorLastName = directors.head._2
        Movie(id.toString, title, year, actors, s"$directorName $directorLastName")
      }
    }
    query.transact(xa)
  }
  val findMovieWithTitleZackSnyderV3 = findMovieByTitleV3("Zack Snyder's Justice League")

  val run: IO[Unit] =
    for {
      _ <- IO.println(dash80.green)
      // _ <- saveAliceAndBob.debug
      _ <- findMovieWithTitleZackSnyderV2.debug
      _ <- IO.println(dash80.green)
    } yield ()
}
