package ScalaChallenge

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}
import org.apache.spark.sql.{SaveMode, DataFrame}
import java.io.File

/*
* This Object purpose is write from spark memory into files into local storage
*
* It could be extended to implement more file writing functions
* */

object WriteUtils {
  def dfToCsv(df: DataFrame, outputDir: String, fileName: String): Unit = {
    val ext = ".csv"
    val tempFile = s"$outputDir/temp/output_$fileName$ext"
    val outputFile = s"$outputDir/output_$fileName$ext"

    // Save the DataFrame as a CSV file in a single partition
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .option("header", true)
      .option("delimiter", ";")
      .csv(tempFile)

    moveAndRenameFile(tempFile, outputFile,s"$outputDir/.output_$fileName$ext.crc",ext)
  }

  def dfToParquet(df: DataFrame, outputDir: String, fileName: String): Unit = {
    val ext = ".parquet"
    val tempFile = s"$outputDir/temp/output_$fileName$ext"
    val outputFile = s"$outputDir/output_$fileName$ext"

    // Save the DataFrame as a Parquet file in a single partition
    df.coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .parquet(tempFile)

    moveAndRenameFile(tempFile, outputFile,s"$outputDir/.output_$fileName$ext.crc",ext)
  }

  private def moveAndRenameFile(sourcePath: String, destinationPath: String,crcPath: String,ext:String): Unit = {
    val hadoopConfig = new Configuration()
    val hdfs = FileSystem.get(hadoopConfig)

    //Get data file without.crc and ignore _SUCCESS file
    val srcFile = FileUtil.listFiles(new File(sourcePath))
      .filter(f => f.getPath.endsWith(ext))(0)

    val destPath = new Path(destinationPath)
    val destFile = new File(destPath.toString)

    // Delete the destination file if it exists
    if (destFile.exists()) {
      destFile.delete()
    }

    // Copy the file outside of the directory and rename it
    FileUtil.copy(srcFile, hdfs, destPath, true, hadoopConfig)

    // Remove the CRC file created from the copy operation
    hdfs.delete(new Path(crcPath), true)

    // Remove the temporary directory created by df.write()
    hdfs.delete(new Path(sourcePath), true)
  }
}