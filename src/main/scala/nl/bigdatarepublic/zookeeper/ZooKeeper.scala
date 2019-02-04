package nl.bigdatarepublic.zookeeper

import java.net.InetSocketAddress
import java.nio.file.Path

import nl.bigdatarepublic.util.FileSystem
import org.apache.zookeeper.server._
import scalaz.zio.IO

sealed trait ZooKeeperInstance {
  def host: String
  def port: Int
  def connectionString: String = s"$host:$port"
}

object ZooKeeper {

  case class Config(port: Int = 2181, tickTime: Int = 1000, maxClientConnexions: Int = 1024)

  private case class ZooKeeperInstanceImpl(cfg: Config, zkDir: Path, f: ServerCnxnFactory) extends ZooKeeperInstance {
    val host: String = "localhost"
    val port: Int = cfg.port
  }

  trait ShutDownFailure
  case class FailedToStopServer(e: Exception) extends ShutDownFailure
  case class FailedToDeleteLogsDir(e: Exception) extends ShutDownFailure

  private def aqcuireServer(cfg: Config): IO[Exception, ZooKeeperInstanceImpl] =
    for {
      zkLogsDir <- FileSystem.createTempDirectory("zookeeper-" + cfg.port)
      rzk       <- IO.syncException {
        val zkServer = new ZooKeeperServer(zkLogsDir.toFile, zkLogsDir.toFile, cfg.tickTime)
        val factory = ServerCnxnFactory.createFactory

        factory.configure(new InetSocketAddress("localhost", cfg.port), cfg.maxClientConnexions)
        factory.startup(zkServer)
        ZooKeeperInstanceImpl(cfg, zkLogsDir, factory)
      }
    } yield rzk

  private def releaseServer(zki: ZooKeeperInstanceImpl): IO[Nothing, Either[Seq[ShutDownFailure], Unit]] = {
    val x = IO
      .syncException { zki.f.shutdown() }
      .bimap(e => FailedToStopServer(e), _ => Unit)
      .attempt

    val y = FileSystem
      .deleteIfExists(zki.zkDir)
      .bimap(e => FailedToDeleteLogsDir(e), _ => Unit)
      .attempt

    for {
      r1 <- x
      r2 <- y
    } yield {
      val s1 = if (r1.isLeft) Seq(r1.left.get) else Seq.empty
      val s2 = if (r2.isLeft) Seq(r2.left.get) else Seq.empty
      val s = s1 ++ s2
      if (s.isEmpty) Right(Unit) else Left(s)
    }
  }

  def startServer(cfg: Config): IO[Exception, ZooKeeperInstance] =
    IO
      .bracket(aqcuireServer(cfg))(releaseServer(_))(zki => IO.syncException(zki))

  def stopServer(server: ZooKeeperInstance): IO[Exception, Unit] = {
    val zk = server.asInstanceOf[ZooKeeperInstanceImpl]
    for {
      _ <- IO.syncException { zk.f.shutdown() }
      _ <- FileSystem.deleteIfExists(zk.zkDir)
    } yield ()
  }

  def withRunningZooKeeper[E, T](cfg: Config = Config())(body: ZooKeeperInstance => IO[E, T]): IO[Any, Unit] = {
    for {
      rzk   <- ZooKeeper.startServer(cfg)
      _     <- body(rzk)
      _     <- ZooKeeper.stopServer(rzk)
    } yield ()
  }
}
