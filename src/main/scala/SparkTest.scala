import java.io.StringWriter

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import scala.collection.JavaConverters._
import scala.collection.immutable.TreeMap

/**
  * Author :sentiYu 
  * Date   :2018-03-29 16:22.
  */
object SparkTest {

  def main(args: Array[String]): Unit = {
    var mainList = List(3, 2, 1)
    var stringBuilder = new StringBuilder
    for (a <- 10 to 20) {
      stringBuilder ++= "\"" + a.toString.concat("\",")
    }
    println(stringBuilder.substring(0, stringBuilder.length - 1))

  }

}


// In test.scala
object Test {
  def main(args: Array[String]): Unit = {
    var xAxisList: List[XAxis] = List()
    for (a <- 1 to 20) {
      var xAxis: XAxis = new XAxis
      xAxis.name = "name".concat(a.toString)
      xAxis.data = List("data1".concat(a.toString))
      val temp = xAxis +: xAxisList
      xAxisList = temp ::: xAxisList
    }

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)
    val out = new StringWriter
    mapper.writeValue(out, xAxisList)
    val json = out.toString

    println(json)

  }
}

object Test2 {
  def main(args: Array[String]) {
    val sites = Map("runoob" -> "www.runoob.com", "google" -> "www.google.com")

    println("show(sites.get( \"runoob\")) : " +
      show(sites.get("runoob")))
    println("show(sites.get( \"baidu\")) : " +
      show(sites.get("baidu")))
  }

  def show(x: Option[String]): String = x match {
    case Some(s) => s
    case None => "?"
  }

}

object TestDecimal {
  def main(args: Array[String]) {
    val data = List("9.8543742472353E-4", "7.057748993232865E-4")
    var result: List[String] = List()
    data.foreach(value => {
      val formatData = "%.2f".format(BigDecimal.apply(value).toDouble)
      val temp: List[String] = result :+ formatData
      result = temp
    })
    println(result)
  }


}

object TestRegex {
  def main(args: Array[String]): Unit = {
    println(isIntByRegex("a123"))
    println(isIntByRegex("1235648.02157"))
    println(isIntByRegex("1235648.02157E10"))
  }

  def isIntByRegex(s: String): Boolean = {
    val regex = """(-)?(\d+)(\.\d*)?""".r
    regex.pattern.matcher(s).matches()
  }
}

object TestMap {
  def main(args: Array[String]): Unit = {
    var map: TreeMap[String, String] = TreeMap()
    map += ("last_total" -> "1", "misc_total" -> "2", "income_total" -> "3")
    println(map)
    println("last_total".hashCode)
    println("misc_total".hashCode)
    println("income_total".hashCode)
  }
}

object TestStringSort {
  def main(args: Array[String]): Unit = {


  }
}
