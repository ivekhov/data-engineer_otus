package com.github.mrpowers.my.cool.project

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf


object JsonReader extends App {

  val conf = new SparkConf().setAppName("JsonReader").setMaster("local")
  val sc =  new SparkContext(conf)

  val dataFileName = args(0)
  val wineData = sc.textFile(s"$dataFileName")

  // local in REPL
  //  val wineData = sc.textFile("winemag-data-130k-v2.json")

  case class Wine(id: Option[Int], country: Option[String], points: Option[Int], title: Option[String], variety: Option[String], winery: Option[String])

  // testing on one row
  //  implicit val formats = DefaultFormats
  //  val line4decode = "{\"id\":0,\"country\":\"Italy\",\"points\":87,\"title\":\"Nicosia 2013 Vulkà Bianco  (Etna)\",\"variety\":\"White Blend\",\"winery\":\"Nicosia\"}"
  //  val decodedWine = parse(line4decode).extract[Wine]
  //  val parsedLine = parse(line4decode)

  // reading file
  println("Start reading..")
  wineData.foreach(s => { implicit val formats = DefaultFormats; println(parse(s).extract[Wine])})
  println("Finished!")

}


