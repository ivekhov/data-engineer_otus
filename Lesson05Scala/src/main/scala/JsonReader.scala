import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object JsonReader extends App {

  val conf = new SparkConf().setAppName("JsonReader").setMaster("local")
  val sc = new SparkContext(conf)

  val dataFileName = args(0)
  val wineData = sc.textFile(s"$dataFileName")

  case class Wine(
                 id: Option[Int],
                 country: Option[String],
                 price: Option[Float], // ?
                 points: Option[Int],
                 title: Option[String],
                 variety: Option[String],
                 winery: Option[String]
                 )
  println("Start reading...")
  wineData.foreach(s => {
    implicit val formats = DefaultFormats;
    println(parse(s).extract[Wine])
  })
  println("Finished")
}

