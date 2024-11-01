package com.tigmaminds.lacrimedataanalysis

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Main{
  def main(stringArgs: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LA Crime Data Analysis")
      .master("local")
      .getOrCreate()

    val startingIndex = 1

    val LACdf = spark.read
      .option("header",value = true)
      .csv("./src/main/resources/LACrimeData/Crime_Data_from_2020_to_Present.csv")

    LACdf.printSchema()

    val CrimeCommited = LACdf
      .select("Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4")
      .withColumn("CrimesId",monotonically_increasing_id() + startingIndex)

//    CrimeCommited.write.

    CrimeCommited.show()
  }
}