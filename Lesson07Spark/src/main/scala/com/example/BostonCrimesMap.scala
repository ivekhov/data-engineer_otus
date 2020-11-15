package com.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.functions.broadcast


object BostonCrimesMap extends App {

  val spark = SparkSession
    .builder()
//    .master("local[*]")
    .getOrCreate()
  def sc = spark.sparkContext

  import spark.implicits._


// prod version - should be uncommented
  // data files from command line
    val dataFileName = args(0)
    val dataFileSecondName = args(1)
    val outputDirName = args(2)

  // dev version - should be commented
//  val dataFileName = "./data/crime.csv"
//  val dataFileSecondName = "./data/offense_codes.csv"
//  val outputDirName = "./output/parquet"


  val crimeFacts = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(dataFileName)

  val crimeNames = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(dataFileSecondName)

  // join with broadcast
  val offenseCodesBroadcast = broadcast(crimeNames)
  val crimeData = crimeFacts
    .join(offenseCodesBroadcast, $"CODE" === $"OFFENSE_CODE")

  crimeData.createOrReplaceTempView("crimeData")

  val districtDF = spark.sql(" WITH t AS ( SELECT DISTRICT, YEAR, MONTH, COUNT(INCIDENT_NUMBER) as crimes  FROM crimeData AS cd GROUP BY cd.DISTRICT, YEAR, MONTH)  SELECT cd.DISTRICT,  count(INCIDENT_NUMBER) AS crimes_total,  percentile_approx(t.crimes, 0.5) AS crimes_monthly,  AVG(Lat) AS lat,  AVG(Long) AS lng  FROM crimeData AS cd LEFT JOIN t ON t.DISTRICT = cd.DISTRICT WHERE cd.DISTRICT is not null GROUP BY cd.DISTRICT")

  val topCrimesInDistricts = spark.sql("WITH t2 AS (SELECT DISTRICT, split(NAME, '-')[0] AS crime_type, COUNT(INCIDENT_NUMBER) as frequency FROM crimeData GROUP BY DISTRICT, crime_type  ) SELECT t2.DISTRICT, t2.crime_type, t2.frequency, RANK(t2.frequency) OVER (PARTITION BY t2.DISTRICT ORDER BY t2.frequency DESC) AS Top FROM t2")

  topCrimesInDistricts.createOrReplaceTempView("topCrimesInDistricts")

  val top3Crimes = spark.sql("SELECT * FROM topCrimesInDistricts WHERE Top <= 3 AND DISTRICT is not null")

  districtDF.coalesce(1).write.parquet(outputDirName)


}
