package warehouse

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

trait SparkSessionWrapper {
  val master = "local[*]"
  val appName = "Warehouse statistics test"

  val conf = new SparkConf()
    .setMaster(master)
    .setAppName(appName)

  lazy val spark = SparkSession.builder().config(conf).getOrCreate()
}
