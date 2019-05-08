

object DemoPivot {

  def main(args: Array[String]): Unit = {
    println(twoSum().mkString)
  }

  def twoSum(): Map[Int, Int] = {
    val arrs = Array(1, 5, 6, 3, 7)
    var map: Map[Int, Int] = Map()
    var resultMap: Map[Int, Int] = Map()
    for (i <- arrs.indices) {
      if (map.contains(9 - arrs(i))) {
        resultMap += (arrs(i) -> i)
      }
      else {
        map += (arrs(i) -> i)
      }
    }
    resultMap
  }
}