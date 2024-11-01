package com.tigmaminds.lacrimedataanalysis

import org.apache.spark.sql.SparkSession

object Main{
  def main(stringArgs: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("LA Crime Data Analysis")
      .master("local")
      .getOrCreate()

    val LACdf = spark.read
      .option("header",true)
      .csv("./src/main/resources/LACrimeData/Crime_Data_from_2020_to_Present.csv")

    LACdf.describe().show()
  }
}