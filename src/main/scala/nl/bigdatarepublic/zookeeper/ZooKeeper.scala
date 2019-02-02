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
    val host = "localhost"
    val port = cfg.port
  }

  def startServer(cfg: Config): IO[Exception, ZooKeeperInstance] =
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
