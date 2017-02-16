package org.spark.test

import scala.collection.mutable.ListBuffer

/**
 * Created by pc on 2016/6/28.
 */
class Test {
  val data = 1
}

object Test {
  def main(args: Array[String]) {
    val t = new Test
    println(t.data)

    val colors = List("red","blue")
    val fiveInt = new Array[Int](5)
    val buf = new ListBuffer[Int]
    buf += 1
    buf += 2
    buf += 3
    val value = "spark://localhost:7077"
    val you = value.stripPrefix("spark://").split(",").map("spark://" + _)
    you.foreach(println)
    val days = List("Sunday=a", "Monday=b")
    parse(days)
    // Make a list element-by-element
    val when = "AM" :: "PM" :: List()
  }

  def parse(list : Seq[String]) : Unit = {
    val OPT = """([a-zA-Z]+)=([a-zA-Z]+)""".r
    // Pattern match
    list match {
      case ("Sunday") :: value :: tail =>
        println("value ---> " + value)
        parse(tail)
      case ("Monday") :: value :: tail =>
        println("value ---> " + value)
        println(tail)
      case OPT(opt, value) :: tail =>
        println(opt + " " + value + " " + tail)
        parse(opt :: value :: tail)
    }
  }
}
