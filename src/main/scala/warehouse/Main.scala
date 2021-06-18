package warehouse


import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}
import warehouse.model.WarehouseStatistics
import warehouse.util.Config.config

object Main {
  val spark: SparkSession = SparkSession.builder.master("local[*]").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val warehousePositions = getWarehousePosition(config("warehousePositionFileName"))
    val warehouseAmounts = getWarehouseAmounts(config("warehouseAmountFileName"))

    firstTask(warehousePositions, warehouseAmounts)
    secondTask(warehousePositions, warehouseAmounts)

    spark.stop()
  }

  def firstTask(warehousePositions: DataFrame, warehouseAmounts: DataFrame): Unit = {
    val startTime = System.currentTimeMillis()

    val currentPositions = WarehouseStatistics.currentPosition(warehousePositions, warehouseAmounts)
    println("Current positions:")
    currentPositions.show()

    val finishTime = System.currentTimeMillis()
    val executedTime = finishTime - startTime
    println(s"Current positions were calculated in $executedTime ms")
  }

  def secondTask(warehousePositions: DataFrame, warehouseAmounts: DataFrame): Unit = {
    val startTime = System.currentTimeMillis()

    val statistics = WarehouseStatistics.statistics(warehousePositions, warehouseAmounts)
    println("Statistic:")
    statistics.show()

    val finishTime = System.currentTimeMillis()
    val executedTime = finishTime - startTime
    println(s"Statistic were calculated in $executedTime ms")
  }

  def getWarehousePosition(filename: String): DataFrame =
    getDataFrameFromCSV(filename, getSchemaForWarehousePosition)

  def getWarehouseAmounts(filename: String): DataFrame =
    getDataFrameFromCSV(filename, getSchemaForWarehouseAmounts)

  private def getDataFrameFromCSV(filename: String, schema: StructType, containsHeader: Boolean = true): DataFrame =
    spark.read.format("csv")
      .option("header", containsHeader.toString)
      .schema(schema)
      .load(filename)

  private def getSchemaForWarehousePosition: StructType = new StructType()
    .add(StructField("positionId", IntegerType, nullable = false))
    .add(StructField("warehouse", StringType, nullable = false))
    .add(StructField("product", StringType, nullable = false))
    .add(StructField("eventTime", LongType, nullable = false))


  private def getSchemaForWarehouseAmounts: StructType = new StructType()
    .add(StructField("positionId", IntegerType, nullable = false))
    .add(StructField("amount", DoubleType, nullable = false))
    .add(StructField("eventTime", LongType , nullable = false))
}
