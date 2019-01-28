package nl.bigdatarepublic.zookeeper

import java.util.concurrent.TimeUnit

import com.twitter.util.{Future => TwitterFuture, Return, Throw}
import scala.concurrent.{Future => ScalaFuture, Promise => ScalaPromise, ExecutionContext}

import nl.bigdatarepublic.kafka._
import org.scalatest._
import com.twitter.util._
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

  import ZooKeeperSpec._

  implicit val timer = new JavaTimer
  private val timeout: Duration = Duration(5L, TimeUnit.SECONDS)

  def withRunningZooKeeper[E, T](body: => IO[E, T]) = {
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
    "be startable" in {
      val io = withRunningZooKeeper {
        import scala.concurrent.ExecutionContext.Implicits.global

        val zkClient = ZkClient("localhost:2128", timeout, timeout)
        IO.fromFuture[ZNode](() =>
          zkClient
            .withAcl(Seq())("/a")
            .create("abc".getBytes)
            .asScala)(global)
      }

      unsafeRun(io)
    }
  }
}
