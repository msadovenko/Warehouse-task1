# Warehouse
## First task from _"Intro to Apache Spark"_ course

### Tasks description
Assume we have a system to store information about warehouses and amounts of goods.
We have the following data with formats:

* List of warehouse positions. Each record for a particular position is unique. Format: { positionId: Long, warehouse: String, product: String, eventTime: Timestamp }
* List of amounts. Records for a particular position can be repeated. The latest record means the current amount. Format: { positionId: Long, amount: BigDecimal, eventTime: Timestamp }.

Using Apache Spark, implement the following methods using DataFrame/Dataset API:
* Load required data from files, e.g. csv of json.
* Find the current amount for each position, warehouse, product.
* Find max, min, avg amounts for each warehouse and product.

#### Example inputs: 
* list of warehouse positions

    |positionId|warehouse|product|eventTime|
    |---|:---:|:---:|:--------:|
    |1 |W-1 |P-1 |1528463098|
    |2 |W-1 |P-2 |1528463100|
    |3 |W-2 |P-3 |1528463110|
    |4 |W-2 |P-4 |1528463111|


* list of amounts (positionId, amount, eventTime)
    
    | positionId |amount|eventTime                |
    |---|------- |--------------------------------|
    |1  | 10.00  | 1528463098  # current amount   |
    |1  | 10.20  | 1528463008                     |
    |2  | 5.00   | 1528463100  # current amount   |
    |3  | 4.90   | 1528463100                     |
    |3  | 5.50   | 1528463111  # current amount   |
    |3  | 5.00   | 1528463105                     |
    |4  | 99.99  | 1528463111	                  |
    |4  | 99.57  | 1528463112  # current amount   |
   
   
#### Outputs: 
* current amount for each position, warehouse, product
    
    |positionId|warehouse|product|amount|
    |---|-------|-------|-----|
    |1  |W-1    |P-1    |10.00|
    |2  |W-1    |P-2    | 5.00|
    ……
    
* max, min, avg amounts for each warehouse and product
    
    |positionId|warehouse|product|max|min|avg|
    |---|-------|-------|-------|-------|-------|
    |1  |W-1    |P-1    |<max> |<min> |<avg> |
    |…..
    |4  |W-2    |P-4    |<max> |<min> |<avg> |

### Implementation

Program consists of 2 files in package _warehouse.model_, and a file Main in package _warehouse_.

#### package warehouse.model
This package includes 2 files:
 * WarehouseInputDataGenerator
 * WarehouseStatistics

<hr>

##### WarehouseInputDataGenerator.scala
It is the file for data generating. The file includes trait Generator[+T]. The trait is a contract for all generators.
The trait involves tree methods:
* generate
* map
* flatmap

Also, there is a _Generators_ object at the file. The object consists following methods and fields:
* _OneDayInMillis_ - number of milliseconds in one day. It may be useful for setting a range of eventTime.

Generators for different type of data:
* _currentTime_
* _integers_
* _longs_
* _floats_

Methods for creation generators which returns a random value between lo and hi:
* _choose(lo: Int, hi: Int): Generator[Int]_
* _choose(lo: Long, hi:Long): Generator[Long]_
* _choose(lo: Float, hi: Float): Generator[Float]_

Methods for input tables:
* _generateWarehousesPosition(count: Int, countOfWarehouses: Int): Seq[(Int, String, String, Long)]_ - returns sequence of positionId,warehouse,product,eventTime
* _warehousesAmount(positionId: Int, startTime: Long, timeCounts: Long, startAmount: Float, maxAmount: Float): Generator[(Int, Float, Long)]_ - returns generator for a warehouse_amount with established positionId.

The object for creating .csv files is _WarehouseInputDataGenerator_. There is a method _createAndFillInputFiles_ for creating and fill input file.
Params:
* positionFileName
* amountFileName
* countOfProduct
* countOfWarehouses
* maxCountOfAmountsForOneProduct - a warehouse can have different products. It is a max count of them.

Finally, there is a DataGeneratorMain with main methods for generation files.

<hr>

##### WarehouseStatistics.scala

There are two public methods for solving tasks:
*  _currentPosition(warehousePositions: DataFrame, warehouseAmounts: DataFrame): DataFrame_ - gets two tables and returns the current amount for each position.
* _statistics(warehousePositions: DataFrame, warehouseAmounts: DataFrame): DataFrame_ - gets two tables and returns their statistics.

<hr>

#### warehouse.Main.scala

It is the file for load dataframes and invoke WarehouseStatistics methods.

Schema for warehouse_position.csv:
```
root
 |-- positionId: integer (nullable = true)
 |-- warehouse: string (nullable = true)
 |-- product: string (nullable = true)
 |-- eventTime: decimal(38,18) (nullable = true)
```

Schema for warehouse_amounts.csv:
```
root
 |-- positionId: integer (nullable = true)
 |-- amount: double (nullable = true)
 |-- eventTime: decimal(38,18) (nullable = true)
```

<hr>