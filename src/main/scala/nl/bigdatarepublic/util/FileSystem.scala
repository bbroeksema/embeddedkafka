package nl.bigdatarepublic.util

import java.io._
import java.nio.file._

import scalaz.zio.IO

trait FileSystem {

  def createTempDirectory(prefix: String): IO[Exception, Path]

  def deleteIfExists(path: Path): IO[Exception, Unit]
}

object NioFileSystem extends FileSystem {

  def createTempDirectory(prefix: String): IO[Exception, Path] =
    IO.syncException {
      val tmpDir = System.getProperty("java.io.tmpdir")
      val path = Paths.get(tmpDir + File.separator + prefix)

      if (Files.exists(path))
        throw new IOException(s"${path.toAbsolutePath.toString} already exists")

      Files.createDirectory(path)
    }

  def deleteIfExists(path: Path): IO[Exception, Unit] = {

    def deleteRecursively(file: File): Unit = {
      if (file.isDirectory)
        file.listFiles.foreach(deleteRecursively)
      if (file.exists && !file.delete)
        throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }

    IO.syncException(deleteRecursively(path.toFile))
  }
}
