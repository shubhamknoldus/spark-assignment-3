package edu.knoldus.application

import edu.knoldus.operation.SparkDataAnalyzer
import org.apache.log4j.Logger
import org.apache.log4j.Level

object ApplicationObject {
  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass)
    val dataAnalyzer = new SparkDataAnalyzer()

    Logger.getLogger("org").setLevel(Level.OFF)

    logger.info(s"\n\n${dataAnalyzer.readInputFile.show}\n\n")
    logger.info(s"\n\n${dataAnalyzer.totalMatchesPlayedAsHome.show}\n\n")
    logger.info(s"\n\n${dataAnalyzer.highestWiningPercentage.show}\n\n")
    logger.info(s"\n\n${dataAnalyzer.convertToDataSet.show}\n\n")
    logger.info(s"\n\n${dataAnalyzer.totalMatchPlayedByEachTeam.show}\n\n")


  }
}
