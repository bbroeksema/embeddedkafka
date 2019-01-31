package nl.bigdatarepublic.zookeeper

import java.util.concurrent.TimeUnit
import scala.concurrent.{ExecutionContext, Promise => ScalaPromise, Future => ScalaFuture}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.collection.JavaConverters._

import nl.bigdatarepublic.kafka._
import org.scalatest._
import com.twitter.util.{Throw, Return, Future => TwitterFuture, _}
import com.twitter.zk._
import nl.bigdatarepublic.util.FileSystem
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

  def withRunningZooKeeper[E, T](body: => IO[E, T]): IO[Any, Unit] = {
    for {
      zkDir <- FileSystem.createTempDirectory("zookeeper")
      zk    <- ZooKeeper.makeServer(zkDir, 2000)
      rzk   <- ZooKeeper.startServer(zk, 2128)
      _     <- body
      _     <- ZooKeeper.stopServer(rzk)
      _     <- FileSystem.deleteIfExists(zkDir)
    } yield ()
  }

  "A ZooKeeper instance" must {
    "be writable and readable" in {
      val io = withRunningZooKeeper {
        val zkClient = ZkClient("localhost:2128", timeout, timeout)
          .withAcl(org.apache.zookeeper.ZooDefs.Ids.OPEN_ACL_UNSAFE.asScala)

        val writeNode = IO.fromFuture[ZNode](() =>
          zkClient("/a")
            .create("abc".getBytes)
            .asScala
        )(global)

        val readNode = IO.fromFuture[Unit](() => {
          val node = zkClient("/a")
          for {
            d <- node.getData().asScala
            _ = d.path shouldBe "/a"
            _ = d.bytes shouldBe "abc".getBytes()
          } yield ()
        })(global)

        for {
          _ <- writeNode
          _ <- readNode
        } yield ()
      }

      unsafeRun(io)
    }
  }
}
