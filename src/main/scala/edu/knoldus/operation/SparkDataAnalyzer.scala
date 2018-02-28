package edu.knoldus.operation

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class TeamRecord(homeTeam: String, awayTeam: String, goalsHomeFT: Int, goalsAwayFT: Int, fullTimeResult: String)

case class TotalMatchPlayed(teamAsHome: String, teamAsAway: String)


class SparkDataAnalyzer {
  val conf = new SparkConf()
    .setMaster("local")
    .setAppName("spark-assignment")

  val sparkSession = SparkSession
    .builder()
    .appName("session-app")
    .config(conf)
    .getOrCreate()
  val csvPath = "/home/knoldus/Desktop/Assignments/spark-assignment-3/src/main/resources/D1.csv"
  val notesPath = "/home/knoldus/Desktop/Assignments/spark-assignment-3/src/main/resources/notes.txt"
  val dataFrameRead = sparkSession
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(csvPath)

  def readInputFile: DataFrame = {
    dataFrameRead
  }

  def totalMatchesPlayedAsHome: DataFrame = {
    dataFrameRead.createOrReplaceTempView("Table")
    sparkSession.sql("select HomeTeam as Team, count(HomeTeam) as Total_Matches from Table group by HomeTeam")
  }
  //Ques 3 and 6
  def highestWiningPercentage: DataFrame = {
    dataFrameRead
      .select("HomeTeam", "FTR")
      .where("FTR == 'H'")
      .groupBy("HomeTeam")
      .count().as("winHome")
      .createOrReplaceTempView("HomeTeamWinView")
    dataFrameRead
      .select("AwayTeam", "FTR")
      .where("FTR == 'A'")
      .groupBy("AwayTeam")
      .count().as("winAway")
      .createOrReplaceTempView("AwayTeamWinView")
    sparkSession.sql("SELECT HomeTeamWinView.HomeTeam, " +
      "(HomeTeamWinView.count + AwayTeamWinView.count) as totalWins " +
      "FROM HomeTeamWinView " +
      "INNER JOIN AwayTeamWinView ON HomeTeamWinView.HomeTeam == AwayTeamWinView.AwayTeam " +
      "order by totalWins DESC LIMIT 10").createOrReplaceTempView("Win_Table")
    sparkSession.sql("SELECT HomeTeam, totalWins as Wins, (totalWins / 0.34) as Win_Percentage FROM Win_Table")
  }

  def convertToDataSet: Dataset[TeamRecord] = {
    import sparkSession.implicits._
    dataFrameRead.map(row => TeamRecord(row.getString(2), row.getString(3), row.getInt(4), row.getInt(5), row.getString(6)))
  }

  def totalMatchPlayedByEachTeam: DataFrame = {
    import sparkSession.implicits._
    dataFrameRead.map(row => TotalMatchPlayed(row.getString(2), row.getString(3)))
      .select("teamAsHome")
      .where("teamAsHome == teamAsHome OR teamAsHome == teamAsAway")
      .groupBy("teamAsHome")
      .count()
  }
}
