import org.apache.spark.sql._

import java.sql.Date
import ScalaChallenge.ChallengeFunctions
import org.scalatest.wordspec.AnyWordSpec

class ChallengeFunctionsSpec extends AnyWordSpec  {

  private val spark: SparkSession = SparkSession.builder()
    .appName("TestApp")
    .master("local[*]")
    .getOrCreate()

  import spark.implicits._

  private val usersData = Seq(
    (1, "Grange Idel", "gidel0@sina.com.cn", "Male", "$5731.67", "12.4.2006"),
    (2, "Calvin Follen", "cfollen1@topsy.com", "Male", "$5741.66", "4.11.1996"),
    (3, "Emile Bearn", "ebearn2@salon.com", "Male", "$2722.4", "14.6.1987"),
    (4, "Calvin Follen", "cfollen1@topsy.com", "Male", "$5741.66", "4.11.1996")
  ).toDF()

  private val moviesData = Seq(
    (4, "€36,20", "Three", "Comedy", 4.1),
    (2, "€18,08", "One", "Drama", 5.0),
    (10, "€36,20", "Nine", "Comedy", 5.0),
    (1, "€12,65", "Ten", "Comedy|Romance", 4.1),
    (13, "€36,20", "10 000 timmar", "Comedy", 0.1),
    (6, "€17,87", "Five", "Action|Crime|Thriller", 4.1),
    (5, "€18,08", "Four", "Drama", 4.7),
    (3, "€17,87", "Two", "Action|Crime|Thriller", 4.1),
    (11, "€18,08", "End of the Affair, The", "Drama", 0.2),
    (12, "€17,87", "Rage", "Action|Crime|Thriller", 0.1),
    (7, "€36,20", "Six", "Comedy", 4.1),
    (8, "€18,08", "Seven", "Drama", 4.1),
    (9, "€17,87", "Eight", "Action|Crime|Thriller", 4.1),
  ).toDF("id", "price", "Title", "Genre", "Score")

  private val today: Date = Date.valueOf("2020-01-01")


  "getTopTenMovies" should {

    val resultDF = ChallengeFunctions.getTopTenMovies(moviesData)

    "return top 10" in {
      // Assert that resultDf contains only 10
      assert(resultDF.count() == 10)
    }

    "Top 10 by score and alphabetically in case of draw" in {
      // Get the expected titles of top 10 movies
      val expectedTitles = Seq("Nine", "One", "Four","Eight", "Five",  "Seven", "Six", "Ten", "Three", "Two")

      // Extract actual titles from the resultDF
      val actualTitles = resultDF.select("Title").collect().map(_.getString(0))

      // Assert that the actual titles match the expected titles and sorting
      assert(actualTitles.toSeq == expectedTitles)
    }
  }
}


