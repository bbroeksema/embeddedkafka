package nl.bigdatarepublic.zookeeper

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
