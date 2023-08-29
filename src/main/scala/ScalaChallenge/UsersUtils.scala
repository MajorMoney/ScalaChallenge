package ScalaChallenge

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

/*
* This Object purpose is to read the Users CSV file and to structure the Users as a class
*
* It could be used to implement Users specific methods
* */

object UsersUtils {
  case class User(id: Int, name: String, email: String, gender: String, money: String, birthdate: String)

  def readUsersData(spark: SparkSession, usersDataPath: String): DataFrame = {
    import spark.implicits._

    val schema = StructType(Seq(
      StructField("id", IntegerType, nullable = false),
      StructField("name", StringType, nullable = false),
      StructField("email", StringType, nullable = false),
      StructField("gender", StringType, nullable = false),
      StructField("money", StringType, nullable = false),
      StructField("birthdate", StringType, nullable = false)
    ))

    val customDateParser: String => java.sql.Date = (dateString: String) => {
      val parts = dateString.split('.')
      val day = parts(0).toInt
      val month = parts(1).toInt
      val year = parts(2).toInt
      java.sql.Date.valueOf(s"$year-$month-$day")
    }
    spark.read
      .option("header", true)
      .schema(schema)
      .csv(usersDataPath)
      .withColumn("birthdate", udf(customDateParser).apply($"birthdate"))
      .as[User]
      .toDF()
  }

}
