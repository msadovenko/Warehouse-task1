package warehouse.model

import java.io.{BufferedWriter, File, FileWriter, PrintWriter}


trait Generator[+T] {
  self =>
  def generate: T

  def map[S](f: T => S): Generator[S]  = new Generator[S] {
    override def generate: S = f(self.generate)
  }

  def flatMap[S](f: T => Generator[S]): Generator[S] = new Generator[S] {
    override def generate: S = f(self.generate).generate
  }
}

object Generators {
  val OneDayInMillis: Int = 24 * 60 * 60 * 1000
  val currentTime: Generator[Long] = new Generator[Long] {
    override def generate: Long = System.currentTimeMillis()
  }

  val integers: Generator[Int] = new Generator[Int] {
    import java.util.Random
    val rand = new Random()

    override def generate: Int = rand.nextInt()
  }

  val longs: Generator[Long] = new Generator[Long] {
    import java.util.Random
    val rand = new Random()

    override def generate: Long = rand.nextLong()
  }

  val floats: Generator[Float] = new Generator[Float] {
    import java.util.Random
    val rand = new Random()

    override def generate: Float = rand.nextFloat()
  }

  def choose(lo: Int, hi: Int): Generator[Int] =
    for(x <- floats) yield (x * (hi - lo)).asInstanceOf[Int] + lo;

  def choose(lo: Long, hi:Long): Generator[Long] =
    for(x <- floats) yield (x * (hi - lo)).asInstanceOf[Long] + lo;

  def choose(lo: Float, hi: Float): Generator[Float] =
    for(x <- floats) yield (x * (hi - lo)) + lo;

  def generateWarehousesPosition(count: Int, countOfWarehouses: Int): Seq[(Int, String, String, Long)] = {
    val warehouseNumberGenerator = choose(1, countOfWarehouses + 1)
    for(i <- 1 to count)
      yield (i, "W-" + warehouseNumberGenerator.generate, s"P-${i}", currentTime.generate)
  }

  def warehousesAmount(positionId: Int, startTime: Long, timeCounts: Long, startAmount: Float, maxAmount: Float): Generator[(Int, Float, Long)] = {
    val timeGenerator = choose(startTime, startTime + timeCounts + 1)
    for (amount <- choose(startAmount, maxAmount))
      yield (positionId, amount, timeGenerator.generate)


  }

}

object DataGenerator {
  def createAndFillInputFiles(positionFileName: String, amountFileName: String)(countOfProduct: Int,
                                                                                countOfWarehouses: Int,
                                                                                maxCountOfAmountsForOneProduct: Int): Unit = {
    val positionWriter = new FileWriter(positionFileName)
    val amountWriter = new FileWriter(amountFileName)

    positionWriter.write("positionId,warehouse,product,eventTime" + System.lineSeparator)
    amountWriter.write("positionId,amount,eventTime" + System.lineSeparator)
    val countOfProductGenerator = Generators.choose(1, maxCountOfAmountsForOneProduct)
    for (warehouse <- Generators.generateWarehousesPosition(countOfProduct, countOfWarehouses)) {
      positionWriter.write(warehouse.productIterator.mkString(",") + System.lineSeparator)
      val warehousesAmountGenerator = Generators
        .warehousesAmount(
          warehouse._1,
          Generators.currentTime.generate,
          Generators.OneDayInMillis,
          5,
          20)
      for (i <- 1 to countOfProductGenerator.generate)
        amountWriter.write(
          warehousesAmountGenerator
            .generate
            .productIterator
            .mkString(",") + System.lineSeparator)
    }
    positionWriter.flush()
    amountWriter.flush()
  }
}

object DataGeneratorMain {
  def main(args: Array[String]): Unit = {
   DataGenerator.createAndFillInputFiles(
     "src/main/resources/warehouse/warehouse_positions.csv",
     "src/main/resources/warehouse/warehouse_amounts.csv")(20, 7, 5)
  }

}

