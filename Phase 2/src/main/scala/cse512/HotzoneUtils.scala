package cse512

object HotzoneUtils {

  //ST_Contains from the previous phase
  def stContains(queryRectangle: String, pointString: String ): Boolean = {
    val point : Array[Double] = pointString.split(",").map(_.trim.toDouble)
    val rectangle : Array[Double] = queryRectangle.split(",").map(_.trim.toDouble)

    val x1 = rectangle(0).min(rectangle(2))
    val x2 = rectangle(0).max(rectangle(2))
    val y1 = rectangle(1).min(rectangle(3))
    val y2 = rectangle(1).max(rectangle(3))

    if (point(0) < x1 || point(0) > x2 || point(1) < y1 || point(1) > y2)
      return false
    else
      return true
  }

}
