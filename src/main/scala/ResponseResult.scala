
class ResponseResult {
  private[this] var msg: String = ""
  private[this] var xAxis: List[XAxis] = _
  private[this] var yAxis: List[YAxis] = _

  def setXAxis(xAxis: List[XAxis]): ResponseResult = {
    this.xAxis = xAxis
    this
  }

  def setYAxis(yAxis: List[YAxis]): ResponseResult = {
    this.yAxis = yAxis
    this
  }

  def getXAxis(): List[XAxis] = {
    this.xAxis
  }

  def getYAxis(): List[YAxis] = {
    this.yAxis
  }

   def getMsg: String = msg

   def setMsg(value: String): Unit = {
    msg = value
  }
}


class XAxis {
  var name: String = ""
  var data: List[String] = _
}

class YAxis {
  var name: String = ""
  var data: List[String] = _
}


class MethodResult[T] {
  var msg: String = ""
  var result: T = _

  def setResult(result: T): Unit = {
    this.result = result
  }

  def getResult(): T = {
    result
  }

  def setMsg(msg: String): Unit = {
    this.msg = msg
  }

  def getMsg(): String = {
    msg
  }
}