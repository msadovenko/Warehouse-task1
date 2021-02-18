package warehouse.model

import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{avg, col, desc, max, min, round, row_number}

object WarehouseStatistics {

  def currentPosition(warehousePositions: DataFrame, warehouseAmounts: DataFrame): DataFrame = {
    val currentAmountPositions = findCurrentAmountForPosition(warehouseAmounts)
    warehousePositions
      .join(currentAmountPositions)
      .where(warehousePositions("positionId") === currentAmountPositions("positionId"))
      .select(
        warehousePositions("positionId"),
        warehousePositions("warehouse"),
        warehousePositions("product"),
        currentAmountPositions("amount")
      )
      .orderBy("positionId")
  }

  def statistics(warehousePositions: DataFrame, warehouseAmounts: DataFrame): DataFrame = {
    val windowSpec =
      Window
        .partitionBy(warehousePositions("positionId"), warehousePositions("warehouse"), warehousePositions("product"))
        .orderBy(warehousePositions("positionId"))

    val windowSpecAgg =
      Window
        .partitionBy(warehousePositions("positionId"), warehousePositions("warehouse"), warehousePositions("product"))

    warehousePositions
      .join(warehouseAmounts)
      .where(warehousePositions("positionId") === warehouseAmounts("positionId"))
      .withColumn("row", row_number().over(windowSpec))
      .withColumn("max", max(col("amount")).over(windowSpecAgg))
      .withColumn("min", min(col("amount")).over(windowSpecAgg))
      .withColumn("avg", round(avg(col("amount")).over(windowSpecAgg), 2))
      .where(col("row") === 1)
      .select(
        warehousePositions("positionId"),
        warehousePositions("warehouse"),
        warehousePositions("product"),
        col("max"),
        col("min"),
        col("avg")
      )
      .orderBy("positionId")
  }

  private def findCurrentAmountForPosition(warehouseAmounts: DataFrame): DataFrame = {
    val windowSpec = Window.partitionBy("positionId").orderBy(desc("eventTime"))
    warehouseAmounts
      .withColumn("row", row_number().over(windowSpec))
      .where(col("row") === 1)
      .select(col("positionId"), col("amount"))
      .orderBy(col("positionId"))
  }
}


