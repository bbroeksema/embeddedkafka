package nl.bigdatarepublic.zookeeper

import java.net.InetSocketAddress
import java.nio.file.Path

import org.apache.zookeeper.server._
import scalaz.zio.IO

sealed trait ZooKeeperInstance { }

object ZooKeeper {

  private case class ZooKeeperInstanceImpl(f: ServerCnxnFactory) extends ZooKeeperInstance

  def makeServer(zkLogsDir: Path, tickTime: Int): IO[Exception, ZooKeeperServer] =
    IO.syncException {
      new ZooKeeperServer(zkLogsDir.toFile, zkLogsDir.toFile, tickTime)
    }

  def startServer(zkServer: ZooKeeperServer, zkPort: Int): IO[Exception, ZooKeeperInstance] =
    IO.syncException {
      val factory: ServerCnxnFactory = ServerCnxnFactory.createFactory
      factory.configure(new InetSocketAddress("localhost", zkPort), 1024)
      factory.startup(zkServer)
      ZooKeeperInstanceImpl(factory)
    }

  def stopServer(server: ZooKeeperInstance): IO[Exception, Unit] =
    IO.syncException {
      server.asInstanceOf[ZooKeeperInstanceImpl].f.shutdown()
    }
}
