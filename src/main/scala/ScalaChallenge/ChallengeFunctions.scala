package ScalaChallenge


import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import java.sql.Date

object ChallengeFunctions {

  def getTopTenMovies(moviesDF: DataFrame): DataFrame = {

    // Select top 10 movies by score, breaking ties alphabetically by title
    val top10MoviesDF = moviesDF
      .orderBy(desc("Score"), asc("Title"))
      .limit(10)
      .toDF()

    top10MoviesDF
  }

  def getBestMoviesByGenre(moviesDF: DataFrame, numMoviesPerGenre: Int): DataFrame = {

    // Parse the genres column and explode it into multiple rows
    val explodedDF = moviesDF
      .withColumn("Genre", explode(split(col("Genre"), "\\|")))

    // Filter out rows with genre "(no genres listed)"
    val filteredDF = explodedDF.filter(col("Genre") =!= "(no genres listed)")

    // Calculate the rank for each movie within each genre
    val rankedDF = filteredDF
      .withColumn("Rank", dense_rank()
        .over(Window.partitionBy("Genre")
          .orderBy(col("Score")
            .desc, col("Title"))))

    // Filter the top 3 movies for each genre
    val topMoviesDF = rankedDF.filter(col("Rank") <= 3)

    // Pivot the topMoviesDF to get separate columns for top 1, top 2, and top 3
    topMoviesDF.groupBy("Genre")
      .pivot("Rank")
      .agg(first("Title"))
      .toDF("Genre", "Top1", "Top2", "Top3")


  }

  def getUsersOverAged(usersDF: DataFrame, today: Date): DataFrame = {

    // Select users over 18
    val filteredUsersDF = usersDF
      .withColumn("birthdate", to_date(col("birthdate"), "dd.MM.yyyy")) // Parse the birthdate column to a DateType
      .withColumn("age", datediff(lit(today), col("birthdate")) / 365.25) // Calculate the age based on the birthdate and today's date
      .filter(col("age") > 18) // Filter out users who are older than 18 years old
      .drop("age")

    filteredUsersDF
  }

  def splitMoneyColumn(usersDF: DataFrame): DataFrame = {

    // Translation table from Char to code
    val currencyMap = Map("$" -> "USD", "€" -> "EUR") // Add more currency symbols and codes as needed

    // Apply translation
    val convertCurrency = udf((money: String) => {
      val currencySymbol = money.charAt(0).toString
      currencyMap.get(currencySymbol)
    })

    // Update the DF
    val updatedUsersDF = usersDF.withColumn("currency", convertCurrency(col("money"))) // Extract the currency and amount from the money column
      .withColumn("amount", expr("substring(money, 2)"))
      .drop("money") // Drop the original money column
      .withColumn("birthdate", to_timestamp(col("birthdate"), "dd.MM.yyyy")) // Convert the birthdate column to timestamp format

    // Select columns in the desired order
    updatedUsersDF.select("id", "name", "email", "gender", "currency", "amount", "birthdate")
  }

  def getAffordableUsers(usersDF: DataFrame, moviesDF: DataFrame): DataFrame = {

    // Remove currency char and cast to Double
    val removeCurrencyChar: String => Double = (priceString: String) => {
      val cleanedString = priceString.replace(",", ".").substring(1)
      cleanedString.toDouble
    }

    // Get the summed value in eur of all movie's
    val totalMovieValue = moviesDF
      .withColumn("price", udf(removeCurrencyChar).apply(col("price")))
      .agg(sum("price"))
      .first()
      .getDouble(0)

    val splitUsersDf = splitMoneyColumn(usersDF)

    // Filter out users that can't buy the totalMovieValue
    val filteredUsersDF = splitUsersDf
      .withColumn("money", when(col("currency").endsWith("USD"), (col("amount") * 0.9) % 0.01)
        .otherwise(col("amount"))) //If user currency is in USD calculate that value in EUR
      .filter(col("money").cast(DoubleType) >= totalMovieValue)
      .drop("money")

    filteredUsersDF
  }

  def filterAvailableMovies(usersDF: DataFrame, moviesDF: DataFrame, overAgedUsersDF: DataFrame): DataFrame = {

    // Get seprated arrays of the possibilities (all movies OR only non horror movies)
    val allMovies = moviesDF.select("Title").collect.map(_.toSeq)
    val nonHorrorMovies = moviesDF.filter(!col("Genre").contains("Horror")).select("Title").collect.map(_.toSeq)

    // Get only the undearged users with anti-join
    val underAgedUsersDF = usersDF.join(overAgedUsersDF.select("id"), Seq("id"), "left_anti")

    // Update "available_movies" value for each user group
    val updatedOverAgedUsersDF = overAgedUsersDF.withColumn("available_movies", lit(allMovies.flatten.mkString("|")))
    val updatedUnderAgedUsersDF = underAgedUsersDF.withColumn("available_movies", lit(nonHorrorMovies.flatten.mkString("|")))

    // Union of the two DF
    updatedOverAgedUsersDF.union(updatedUnderAgedUsersDF)
  }


}
