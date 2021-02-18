package warehouse

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DecimalType, DoubleType, IntegerType, StringType, StructField, StructType}
import warehouse.model.WarehouseStatistics

object Main {
  val spark: SparkSession = SparkSession.builder.master("local").getOrCreate
  spark.sparkContext.setLogLevel("ERROR")

  def main(args: Array[String]): Unit = {
    val warehousePositions = getWarehousePosition("src/main/resources/warehouse/warehouse_positions.csv")
    val warehouseAmounts = getWarehouseAmounts("src/main/resources/warehouse/warehouse_amounts.csv")

    val currentPositions = WarehouseStatistics.currentPosition(warehousePositions, warehouseAmounts)
    println("Current positions:")
    currentPositions.show()

    val statistics = WarehouseStatistics.statistics(warehousePositions, warehouseAmounts)
    println("Statistics:")
    statistics.show()

    spark.stop()
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
    .add(StructField("eventTime", DecimalType.SYSTEM_DEFAULT , nullable = false))


  private def getSchemaForWarehouseAmounts: StructType = new StructType()
    .add(StructField("positionId", IntegerType, nullable = false))
    .add(StructField("amount", DoubleType, nullable = false))
    .add(StructField("eventTime", DecimalType.SYSTEM_DEFAULT , nullable = false))
}
