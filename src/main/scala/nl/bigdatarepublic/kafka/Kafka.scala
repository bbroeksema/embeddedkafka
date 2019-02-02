package nl.bigdatarepublic.kafka

import java.nio.file.Path

import scala.collection.JavaConverters._

import kafka.server._
import org.apache.kafka.common.security.auth.SecurityProtocol
import scalaz.zio.IO

import nl.bigdatarepublic.util.FileSystem

sealed trait KafkaBrokerInstance {
  def host: String

  def port: Int

  def connectionString: String = s"$host:$port"
}

object Kafka {

  case class Config(zooKeeperPort: Int = 2181,
                    kafkaPort: Int = 9092,
                    customBrokerProperties: Map[String, String] = Map.empty)

  private case class KafkaServerInstanceImpl(cfg: Config,
                                             kafkaLogsDir: Path,
                                             s: KafkaServer) extends KafkaBrokerInstance {
    val host = "localhost"
    val port = cfg.kafkaPort
  }

  def startServer(kafkaLogsDir: Path, config: Config): IO[Exception, Unit] = {
    for {
      kafkaLogsDir <- FileSystem.createTempDirectory(s"kafka-${config.kafkaPort}")
      kafkaServer  <- IO.syncException {
        val zkAddress = s"localhost:${config.zooKeeperPort}"
        val listener = s"${SecurityProtocol.PLAINTEXT}://localhost:${config.kafkaPort}"

        val brokerProperties = Map[String, Object](
          KafkaConfig.ZkConnectProp -> zkAddress,
          KafkaConfig.BrokerIdProp -> 0.toString,
          KafkaConfig.ListenersProp -> listener,
          KafkaConfig.AdvertisedListenersProp -> listener,
          KafkaConfig.AutoCreateTopicsEnableProp -> true.toString,
          KafkaConfig.LogDirProp -> kafkaLogsDir.toAbsolutePath,
          KafkaConfig.LogFlushIntervalMessagesProp -> 1.toString,
          KafkaConfig.OffsetsTopicReplicationFactorProp -> 1.toString,
          KafkaConfig.OffsetsTopicPartitionsProp -> 1.toString,
          KafkaConfig.TransactionsTopicReplicationFactorProp -> 1.toString,
          KafkaConfig.TransactionsTopicMinISRProp -> 1.toString,
          // The total memory used for log deduplication across all cleaner threads,
          // keep it small to not exhaust suite memory
          KafkaConfig.LogCleanerDedupeBufferSizeProp -> 1048577.toString
        ) ++ config.customBrokerProperties

        val server = new KafkaServer(new KafkaConfig(brokerProperties.asJava))
        server.startup()

        KafkaServerInstanceImpl(config, kafkaLogsDir, server)
      }
    } yield kafkaServer
  }

  def stopServer(server: KafkaServer): IO[Exception, Unit] = {
    val kafkaServer = server.asInstanceOf[KafkaServerInstanceImpl]
    for {
      _ <- IO.syncException { kafkaServer.s.shutdown() }
      _ <- FileSystem.deleteIfExists(kafkaServer.kafkaLogsDir)
    } yield ()
  }
}
