package ScalaChallenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


/*
* This Object purpose is to read the Movies CSV file and to structure the Movies as a class
*
* It could be used to implement Movies specific methods
* */

object MoviesUtils {
  case class Movie(id: Int, price: String, title: String, genre: String, score: Double)

  def readMoviesData(spark: SparkSession, moviesDataPath: String): DataFrame = {
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("price", StringType, nullable = false),
      StructField("Title", StringType, nullable = false),
      StructField("Genre", StringType, nullable = false),
      StructField("Score", DoubleType, nullable = false)
    ))

    val removeQuotes: String => String = (title: String) => Option(title).getOrElse("").replaceAll("\"", "")

    spark.read
      .option("header", true)
      .schema(schema)
      .csv(moviesDataPath)
      .withColumn("Title", udf(removeQuotes).apply(col("Title")))
      .as[Movie]
      .toDF()
  }

}