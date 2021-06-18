package warehouse

import org.apache.spark.sql.DataFrame
import org.scalatest.{BeforeAndAfterAll, PrivateMethodTester, stats}
import org.scalatest.wordspec.AnyWordSpec
import warehouse.model.WarehouseStatistics

class WarehouseStatisticsTest extends AnyWordSpec with BeforeAndAfterAll with PrivateMethodTester with SparkSessionWrapper {
  import spark.sqlContext.implicits._

  var warehouseAmounts: DataFrame = _
  var warehousePositions: DataFrame = _
  var currentPositionExpected: DataFrame = _
  var statisticsExpected: DataFrame = _


  override def beforeAll(): Unit = {
    setWarehouseAmount()
    setWarehousePosition()
    setCurrentPositionExpected()
    setStatisticsExpected()
  }

  private def setWarehouseAmount(): Unit = {
    warehouseAmounts = Seq(
      (1,19.0,1624049943351L),
      (1,16.0,1624093233891L),
      (1,10.3,1624018103968L),
      (2,16.0,1624037335470L),
      (2,5.0,1624066326326L),
      (2,7.0,1624038997102L),
      (2,9.0,1624094151086L),
      (3,12.0,1624023216877L),
      (3,13.0,1624090399951L),
      (3,17.0,1624052262555L),
      (4,17.0,1624040012529L),
      (4,16.0,1624018531062L),
      (4,15.0,1624057711451L),
      (5,15.0,1624061005187L),
      (5,16.0,1624065858723L),
      (5,6.0,1624037596537L),
      (5,9.0,1624012518189L),
      (6,14.0,1624068528376L),
      (6,10.0,1624096437952L),
      (7,13.0,1624062698352L),
      (8,13.0,1624031532378L),
      (8,16.0,1624068406840L),
      (9,8.0,1624050900456L),
      (10,5.0,1624020145317L)
    ).toDF("positionId", "amount", "eventTime")
  }

  private def setWarehousePosition(): Unit = {
    warehousePositions = Seq(
      (1,"W-1", "P-1", 1624012291440L),
      (2,"W-4", "P-2", 1624012291440L),
      (3,"W-2", "P-3", 1624012291440L),
      (4,"W-7", "P-4", 1624012291440L),
      (5,"W-4", "P-5", 1624012291440L),
      (6,"W-2", "P-6", 1624012291440L),
      (7,"W-6", "P-7", 1624012291440L),
      (8,"W-7", "P-8", 1624012291440L),
      (9,"W-5", "P-9", 1624012291440L),
      (10,"W-5" ,"P-10", 1624012291440L)
    ).toDF("positionId", "warehouse", "product", "eventTime")
  }

  private def setCurrentPositionExpected(): Unit = {
    currentPositionExpected = Seq(
      (1, "W-1", "P-1", 16.0),
      (2, "W-4", "P-2", 9.0),
      (3, "W-2", "P-3", 13.0),
      (4, "W-7", "P-4", 15.0),
      (5, "W-4", "P-5", 16.0),
      (6, "W-2", "P-6", 10.0),
      (7, "W-6", "P-7", 13.0),
      (8, "W-7", "P-8", 16.0),
      (9, "W-5", "P-9", 8.0),
      (10, "W-5", "P-10", 5.0)
    ).toDF("positionID", "warehouse", "product", "amount")
  }

  private def setStatisticsExpected(): Unit = {
    statisticsExpected = Seq(
      (1, "W-1", "P-1", 19.0, 10.3, 15.1),
      (2, "W-4", "P-2", 16.0, 5.0, 9.25),
      (3, "W-2", "P-3", 17.0, 12.0, 14.0),
      (4, "W-7", "P-4", 17.0, 15.0, 16.0),
      (5, "W-4", "P-5", 16.0, 6.0, 11.5),
      (6, "W-2", "P-6", 14.0, 10.0, 12.0),
      (7, "W-6", "P-7", 13.0, 13.0, 13.0),
      (8, "W-7", "P-8", 16.0, 13.0, 14.5),
      (9, "W-5", "P-9", 8.0, 8.0, 8.0),
      (10, "W-5", "P-10", 5.0, 5.0, 5.0)
    ).toDF("positionId", "warehouse", "product", "max", "min", "avg")
  }

  "Correct compute current positions" in {
    val currentPosition = WarehouseStatistics.currentPosition(warehousePositions, warehouseAmounts)
    assert(currentPosition.schema.equals(currentPositionExpected.schema))
    assert(currentPosition.collect().sameElements(currentPositionExpected.collect()))
  }

  "Correct compute statistics" in {
    val statistics = WarehouseStatistics.statistics(warehousePositions, warehouseAmounts)
    assert(statistics.schema.fieldNames.sameElements(statisticsExpected.schema.fieldNames))
    statistics.show()
    statisticsExpected.show()
    assert(statistics.collect().sameElements(statisticsExpected.collect()))
  }

  override def afterAll() {
    if (spark != null) {
      spark.stop()
    }
  }

}
