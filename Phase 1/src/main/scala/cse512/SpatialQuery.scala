package cse512

import org.apache.spark.sql.SparkSession

object SpatialQuery extends App{

  def stContains(queryRectangle: String, pointString: String ): Boolean = {

    //Extract point and rectangle co-ordinates
    val point : Array[Double] = pointString.split(",").map(_.trim.toDouble)
    val rectangle : Array[Double] = queryRectangle.split(",").map(_.trim.toDouble)

    val x1 = rectangle(0).min(rectangle(2))
    val x2 = rectangle(0).max(rectangle(2))
    val y1 = rectangle(1).min(rectangle(3))
    val y2 = rectangle(1).max(rectangle(3))

    //Checks if the point of interest is inside the given rectangle
    if (point(0) < x1 || point(0) > x2 || point(1) < y1 || point(1) > y2)
      return false
    else
      return true
  }

  def stWithin(pointString1: String, pointString2: String, distance: Double): Boolean = {

    //Extracts the point co-ordinates
    val point1: Array[Double] = pointString1.split(",").map(_.trim.toDouble)
    val point2 : Array[Double] = pointString2.split(",").map(_.trim.toDouble)

    val distancep1p2 = math.sqrt((point1(0)-point2(0))*(point1(0)-point2(0)) + (point1(1)-point2(1))*(point1(1)-point2(1)))

    return distancep1p2 <= distance
  }

  def runRangeQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((stContains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from point where ST_Contains('"+arg2+"',point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runRangeJoinQuery(spark: SparkSession, arg1: String, arg2: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    val rectangleDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    rectangleDf.createOrReplaceTempView("rectangle")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Contains",(queryRectangle:String, pointString:String)=>((stContains(queryRectangle, pointString))))

    val resultDf = spark.sql("select * from rectangle,point where ST_Contains(rectangle._c0,point._c0)")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((stWithin(pointString1, pointString2, distance))))

    val resultDf = spark.sql("select * from point where ST_Within(point._c0,'"+arg2+"',"+arg3+")")
    resultDf.show()

    return resultDf.count()
  }

  def runDistanceJoinQuery(spark: SparkSession, arg1: String, arg2: String, arg3: String): Long = {

    val pointDf = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg1);
    pointDf.createOrReplaceTempView("point1")

    val pointDf2 = spark.read.format("com.databricks.spark.csv").option("delimiter","\t").option("header","false").load(arg2);
    pointDf2.createOrReplaceTempView("point2")

    // YOU NEED TO FILL IN THIS USER DEFINED FUNCTION
    spark.udf.register("ST_Within",(pointString1:String, pointString2:String, distance:Double)=>((stWithin(pointString1, pointString2, distance))))
    val resultDf = spark.sql("select * from point1 p1, point2 p2 where ST_Within(p1._c0, p2._c0, "+arg3+")")
    resultDf.show()

    return resultDf.count()
  }
}
