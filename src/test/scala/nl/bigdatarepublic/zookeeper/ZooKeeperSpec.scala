package nl.bigdatarepublic.zookeeper

import java.io.File
import java.nio.file.Paths
import java.util.concurrent.TimeUnit

import scala.concurrent.{ExecutionContext, Promise => ScalaPromise, Future => ScalaFuture}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

import org.scalatest._
import com.twitter.util.{Throw, Return, Future => TwitterFuture, _}
import com.twitter.zk._
import scalaz.zio._
import scalaz.zio.interop.future._

object ZooKeeperSpec {
  implicit class RichTwitterFuture[A](val tf: TwitterFuture[A]) extends AnyVal {
    def asScala(implicit e: ExecutionContext): ScalaFuture[A] = {
      val promise: ScalaPromise[A] = ScalaPromise()
      tf.respond {
        case Return(value) => promise.success(value)
        case Throw(exception) => promise.failure(exception)
      }
      promise.future
    }
  }
}

class ZooKeeperSpec extends WordSpec with Matchers with RTS {

  import nl.bigdatarepublic.zookeeper.ZooKeeperSpec._

  implicit val timer: JavaTimer = new JavaTimer
  private val timeout: Duration = Duration(5L, TimeUnit.SECONDS)

  "A ZooKeeper instance" must {
    "be writable and readable" in {
      val io = ZooKeeper.withRunningZooKeeper() { zookeeper =>
        val zkClient = ZkClient(zookeeper.connectionString, timeout, timeout)
          .withAcl(org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)

        for {
          _ <- writeNode(zkClient, "/a", "abc")
          _ <- readNode(zkClient, "/a", "abc")
        } yield ()
      }

      unsafeRun(io)
    }
  }

  "multiple zookeeper instances" must {
    val cfg1 = ZooKeeper.Config(port = 2182)
    val cfg2 = ZooKeeper.Config(port = 2183)

    "running intertwined without problems" in {

      val io = for {
        zk1  <- ZooKeeper.startServer(cfg1)
        zkc1 = zkClientFromInstance(zk1)

        _    <- writeNode(zkc1, "/a", "abc")

        zk2  <- ZooKeeper.startServer(cfg2)
        zkc2 = zkClientFromInstance(zk2)

        _    <- writeNode(zkc1, "/b", "def")
        _    <- writeNode(zkc2, "/c", "ghi")

        _    <- readNode(zkc1, "/b", "def")
        _    <- readNode(zkc2, "/c", "ghi")


        _    <- ZooKeeper.stopServer(zk2)

        _    <- readNode(zkc1, "/a", "abc")

        _    <- ZooKeeper.stopServer(zk1)
      } yield ()


      unsafeRun(io)
    }

    "properly clean up in case of exceptions" in {
      val io = (for {
        zk1  <- ZooKeeper.startServer(cfg1)
        zk2  <- ZooKeeper.startServer(cfg1) // Shouldn't work as there is already one running on 2182
      } yield ()).attempt.map { either =>
        either.isLeft shouldBe true

        val tmpDir = System.getProperty("java.io.tmpdir")
        val path = Paths.get(tmpDir + File.separator + "zookeeper-" + cfg1.port)
        path.toFile.exists() shouldBe false
      }

      unsafeRun(io)
    }
  }

  private def zkClientFromInstance(zki: ZooKeeperInstance): ZkClient = {
    ZkClient(zki.connectionString, timeout, timeout)
      .withAcl(org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)
  }

  private def writeNode(zkc: ZkClient, path: String, data: String): IO[Throwable, ZNode] = {
    IO.fromFuture[ZNode](() =>
      zkc(path)
        .create(data.getBytes)
        .asScala
    )(global)
  }

  private def readNode(zkc: ZkClient, path: String, expectedData: String): IO[Throwable, Unit] =
    IO.fromFuture[Unit](() => {
      val node = zkc(path)
      for {
        d <- node.getData().asScala
        _ = d.path shouldBe path
        _ = d.bytes shouldBe expectedData.getBytes()
      } yield ()
    })(global)
}
