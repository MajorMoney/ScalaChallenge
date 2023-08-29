package ScalaChallenge

import org.apache.spark.sql.SparkSession
import ChallengeFunctions._
import ScalaChallenge.WriteUtils.{dfToCsv, dfToParquet}
import ScalaChallenge.MoviesUtils.readMoviesData
import ScalaChallenge.UsersUtils.readUsersData
/*
*Program entry point/main it starts the spark session
* and chain call all necessary functions to solve th challenge tasks
* */
object SparkProgram {

  def main(Args:Array[String]):Unit={

    val moviesFile="mock_data_movies.csv"
    val usersFile="mock_data_users.csv"
    val outputDir="Output"

    val spark=SparkSession.builder().appName("SparkProgram").master("local[*]").getOrCreate()

    val moviesDF = readMoviesData(spark, moviesFile)
    val usersDF = readUsersData(spark, usersFile)

    //Task1
    val top10MoviesDF=getTopTenMovies( moviesDF)
    dfToCsv(top10MoviesDF, outputDir, "1")

    //Task2
    val top3MoviesGenreDF=getBestMoviesByGenre(moviesDF,3)
    dfToCsv(top3MoviesGenreDF, outputDir, "2")

    //Task3
    val over18UsersDF=getUsersOverAged(usersDF,java.sql.Date.valueOf("2020-01-01"))
    dfToCsv(over18UsersDF, outputDir, "3")

    //Task4
    val splitCurrencyDF=splitMoneyColumn(usersDF)
    dfToParquet(splitCurrencyDF, outputDir, "4")

    //Task5
    val affordableUsersDF=getAffordableUsers(usersDF,moviesDF)
    dfToCsv(affordableUsersDF,outputDir,"5")

    //Task6
    val availableMovies=filterAvailableMovies(usersDF,moviesDF,over18UsersDF)
    dfToParquet(availableMovies,outputDir,"6")

    spark.stop()
  }

}
