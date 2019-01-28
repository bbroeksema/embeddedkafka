name := "embededkafka"

version := "0.1"

scalaVersion := "2.12.8"

val scalazZioVersion = "0.5.3"

libraryDependencies ++= Seq(
  "org.scalaz"       %% "scalaz-zio"               % scalazZioVersion,
  "org.apache.kafka" %% "kafka"                    % "2.0.0",

  "net.manub"        %% "scalatest-embedded-kafka"  % "2.0.0"          % "test",
  "org.scalatest"    %% "scalatest"                 % "3.0.5"          % "test",
  "com.twitter"      %% "util-zk"                   % "19.1.0"         % "test",
  "org.scalaz"       %% "scalaz-zio-interop-future" % scalazZioVersion % "test"
)
