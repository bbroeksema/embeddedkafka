package nl.bigdatarepublic.kafka

import java.io.File
import java.nio.file._

import scalaz.zio.IO


object FileSystem {

  /**
    * IllegalArgumentException - if the prefix cannot be used to generate a
    *   candidate directory name
    * UnsupportedOperationException - if the array contains an attribute that
    *   cannot be set atomically when creating the directory
    * IOException - if an I/O error occurs or the temporary-file directory does
    *   not exist
    * SecurityException - In the case of the default provider, and a security
    *   manager is installed, the checkWrite method is invoked to check write access when creating the directory.
    *
    * @param prefix
    * @return
    */
  def createTempDirectory(prefix: String): IO[Exception, Path] =
    IO.syncException {
      val path = Files.createTempDirectory(prefix)
      Files.deleteIfExists(path)
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
