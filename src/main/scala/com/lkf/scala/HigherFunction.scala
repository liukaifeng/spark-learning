package com.lkf.scala

/**
  * todo 一句话描述该类的用途
  *
  * @author 刘凯峰
  * @date 2019-11-20 14-27
  */
object HigherFunction {
  def main(args: Array[String]): Unit = {

  }
  import scala.util.Random

  class Stack[A] {
    private var elements: List[A] = Nil
    def push(x: A) { elements = x :: elements }
    def peek: A = elements.head
    def pop(): A = {
      val currentTop = peek
      elements = elements.tail
      currentTop
    }
  }

  class Fruit
  class Apple extends Fruit
  class Banana extends Fruit

  val stack = new Stack[Fruit]
  val apple = new Apple
  val banana = new Banana

  stack.push(apple)
  stack.push(banana)
}
