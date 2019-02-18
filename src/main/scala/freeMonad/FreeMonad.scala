package freeMonad

object FreeMonad {}

trait Monad[F[_]] {

  def flatMap[A, B](eff: F[A])(f: A => F[B]): F[B]
  def map[A, B](eff: F[A])(f: A => B): F[B] =
    lift(f)(eff)

  def lift[A, B](f: A => B): F[A] => F[B]

}

object Monad {

  implicit val optionMonad = new Monad[Option] {
    override def flatMap[A, B](eff: Option[A])(f: A => Option[B]): Option[B] =
      eff.flatMap(f)

    override def lift[A, B](f: A => B): Option[A] => Option[B] =
      opt => opt.map(f)
  }

  implicit val taskMonad = new Monad[Task] {

    override def flatMap[A, B](eff: Task[A])(f: A => Task[B]): Task[B] =
      f(eff.run())

    override def lift[A, B](f: A => B): Task[A] => Task[B] =
      taskA => Task(() => f(taskA.run()))
  }

  def apply[F[_]](implicit monadF: Monad[F]): Monad[F] = new Monad[F] {

    override def flatMap[A, B](eff: F[A])(f: A => F[B]): F[B] =
      monadF.flatMap(eff)(f)

    override def lift[A, B](f: A => B): F[A] => F[B] =
      monadF.lift(f)

  }

  implicit class MonadOps[F[_], A](eff: F[A])(implicit monadF: Monad[F]) {

    def flatMap[B](f: A => F[B]): F[B] =
      monadF.flatMap(eff)(f)

    def map[B](f: A => B): F[B] =
      monadF.map(eff)(f)

  }

}

object Main {

  import Monad._

  def main(args: Array[String]): Unit = {

    val g = (n: Int) => n * n

    val lifted = Monad[Option].lift(g)

    println(lifted(Some(5)))

    val result = for {
      five <- Task(() => 5)
      six <- Task(() => 6)
    } yield five + six

    println(result.run())

  }

}

case class Task[A](run: () => A)
