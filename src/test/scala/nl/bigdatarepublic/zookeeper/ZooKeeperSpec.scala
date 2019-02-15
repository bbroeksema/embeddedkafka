package nl.bigdatarepublic.zookeeper

import java.io.IOException
import java.nio.file.Path
import java.util.concurrent.TimeUnit

import com.twitter.util.{Duration, JavaTimer, Return, Throw, Future => TwitterFuture}
import com.twitter.zk._
import nl.bigdatarepublic.util.FileSystem
import org.scalatest._
import scalaz.zio._
import scalaz.zio.interop.future._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.{ExecutionContext, Future => ScalaFuture, Promise => ScalaPromise}

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

  "ZooKeeper.withRunningZooKeeper" when {
    "a temp dir can be created" must {
      "provide a writable and readable ZooKeeper instance" in {
        val io = DefaultZooKeeper.withRunningZooKeeper() { zookeeper =>
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

    "no temp dir can be created" must {
      val mockException = new IOException("Mock exception")
      val failingFileSystem = new FileSystem {
        override def createTempDirectory(prefix: String): IO[Exception, Path] = IO.fail(mockException)

        override def deleteIfExists(path: Path): IO[Exception, Unit] = IO.fail(mockException)
      }

      "return the inner exception" in {
        val io = new ZooKeeper(failingFileSystem).withRunningZooKeeper() { _ =>
          IO.now(Unit)
        }

        unsafeRun(io.attempt) should equal(Left(mockException))
      }

      "not try to delete the temp dir" in {
        val zk = new ZooKeeper(new FileSystem {
          override def createTempDirectory(prefix: String): IO[Exception, Path] = IO.fail(mockException)

          override def deleteIfExists(path: Path): IO[Exception, Unit] = IO.fail {
            assert(false, "deleteIfExists should not be called")
            mockException
          }
        })

        val io = zk.withRunningZooKeeper() { _ =>
          IO.now(Unit)
        }

        io.attempt
      }

      "not try to create a ZooKeeper instance" in {
        // TODO
      }
    }
  }

  "Multiple zookeeper instances" must {
    val cfg1 = ZooKeeper.Config(port = 2182)
    val cfg2 = ZooKeeper.Config(port = 2183)

    "run intertwined without problems" in {
      val io = for {
        zk1  <- DefaultZooKeeper.startServer(cfg1)
        zkc1 = zkClientFromInstance(zk1)

        _    <- writeNode(zkc1, "/a", "abc")

        zk2  <- DefaultZooKeeper.startServer(cfg2)
        zkc2 = zkClientFromInstance(zk2)

        _    <- writeNode(zkc1, "/b", "def")
        _    <- writeNode(zkc2, "/c", "ghi")

        _    <- readNode(zkc1, "/b", "def")
        _    <- readNode(zkc2, "/c", "ghi")


        _    <- DefaultZooKeeper.stopServer(zk2)

        _    <- readNode(zkc1, "/a", "abc")

        _    <- DefaultZooKeeper.stopServer(zk1)
      } yield ()


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
