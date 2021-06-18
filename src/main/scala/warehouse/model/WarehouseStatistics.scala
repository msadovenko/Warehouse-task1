package warehouse.model

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions.{avg, col, desc, max, min, round, row_number}

object WarehouseStatistics {

  def currentPosition(warehousePositions: DataFrame, warehouseAmounts: DataFrame): DataFrame = {
    val currentAmountPositions = findCurrentAmountForPosition(warehouseAmounts)
    warehousePositions
      .join(currentAmountPositions, warehousePositions("positionId") === currentAmountPositions("positionId"))
      .select(
        currentAmountPositions("positionID"),
        warehousePositions("warehouse"),
        warehousePositions("product"),
        currentAmountPositions("amount")
      )
      .orderBy("positionId")
  }

  def statistics(warehousePositions: DataFrame, warehouseAmounts: DataFrame): DataFrame = {
    val aggAmountDF = warehouseAmounts
      .groupBy(col("positionID"))
      .agg(
        max(col("amount")).as("max"),
        min(col("amount")).as("min"),
        avg(col("amount")).as("avg"))
      .select("positionID", "avg", "min", "max")

    val aggregatedDF = aggAmountDF
      .join(warehousePositions, warehousePositions("positionID") === aggAmountDF("positionID"))
      .select(
        warehousePositions("positionId"),
        warehousePositions("warehouse"),
        warehousePositions("product"),
        col("max"),
        col("min"),
        col("avg")
      )
      .orderBy("positionId")
    aggregatedDF
  }

  private def findCurrentAmountForPosition(warehouseAmounts: DataFrame): DataFrame = {
    val windowSpec: WindowSpec = Window.partitionBy("positionId")
    warehouseAmounts
      .select(
        col("positionId"),
        col("amount"),
        max(col("eventTime")).over(windowSpec).as("last_time")
      )
      .filter(col("eventTime") === col("last_time"))

  }
}


