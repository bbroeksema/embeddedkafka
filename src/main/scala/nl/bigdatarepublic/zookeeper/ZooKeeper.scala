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

  private case class ZooKeeperInstanceImpl(cfg: Config, f: ServerCnxnFactory) extends ZooKeeperInstance {
    val host = "localhost"
    val port = cfg.port
  }

  def makeServer(zkLogsDir: Path, tickTime: Int): IO[Exception, ZooKeeperServer] =
    IO.syncException {
      new ZooKeeperServer(zkLogsDir.toFile, zkLogsDir.toFile, tickTime)
    }

  def startServer(zkServer: ZooKeeperServer, cfg: Config): IO[Exception, ZooKeeperInstance] =
    IO.syncException {
      val factory: ServerCnxnFactory = ServerCnxnFactory.createFactory
      factory.configure(new InetSocketAddress("localhost", cfg.port), cfg.maxClientConnexions)
      factory.startup(zkServer)
      ZooKeeperInstanceImpl(cfg, factory)
    }

  def stopServer(server: ZooKeeperInstance): IO[Exception, Unit] =
    IO.syncException {
      server.asInstanceOf[ZooKeeperInstanceImpl].f.shutdown()
    }

  def withRunningZooKeeper[E, T](cfg: Config = Config())(body: ZooKeeperInstance => IO[E, T]): IO[Any, Unit] = {
    for {
      zkDir <- FileSystem.createTempDirectory("zookeeper")
      zk    <- ZooKeeper.makeServer(zkDir, cfg.tickTime)
      rzk   <- ZooKeeper.startServer(zk, cfg)
      _     <- body(rzk)
      _     <- ZooKeeper.stopServer(rzk)
      _     <- FileSystem.deleteIfExists(zkDir)
    } yield ()
  }
}
