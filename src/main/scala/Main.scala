package com.tigmaminds.lacrimedataanalysis

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import java.util.Properties

object Main{
  def main(stringArgs: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("LA Crime Data Analysis")
      .config("spark.driver.maxResultSize", "1g")
      .config("spark.driver.memory", "4g")
      .master("local")
      .getOrCreate()

    val jdbcUrl = "jdbc:mysql://localhost:3306/LACrimes?allowPublicKeyRetrieval=true"
    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", "root")
    jdbcProperties.setProperty("password", "root")
    jdbcProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")

    val startingIndex = 1

    val LACdf = spark.read
      .option("header",value = true)
      .csv("./src/main/resources/LACrimeData/Crime_Data_from_2020_to_Present.csv")

    LACdf.printSchema()

    val CrimeCommitedDf = LACdf
      .select("Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4")
      .withColumn("CrimesId",generateIndex())

    val CrimeDf = LACdf.select("Crm Cd","Crm Cd Desc").dropDuplicates()

    val ReportedDf = LACdf.select("Date Rptd", "Rpt Dist No")
      .withColumn("ReportedId",generateIndex())

    val OccationDf = LACdf.select("DATE OCC","TIME OCC")
      .withColumn("OccationId",generateIndex())

    val WeaponDf = LACdf.select("Weapon Used Cd", "Weapon Desc").dropDuplicates()

    val AREADf = LACdf.select("AREA", "AREA NAME").dropDuplicates()

    val Victim = LACdf.select("Vict Age","Vict Sex","Vict Descent")
      .withColumn("VictimId",generateIndex())

    val Premis = LACdf.select("Premis Cd", "Premis Desc").dropDuplicates()

    val Status = LACdf.select("Status", "Status Desc").dropDuplicates()

    val factDf = LACdf.select("DR_NO","Part 1-2","Mocodes","LOCATION","Cross Street","LAT","LON","Crm Cd 1","Crm Cd Desc")
      .join(CrimeCommitedDf.select("Crm Cd 1","CrimesId"),Seq("Crm Cd 1"),"left")
      .drop("Crm Cd 1")

    CrimeCommitedDf.printSchema()
    CrimeDf.printSchema()
    ReportedDf.printSchema()
    OccationDf.printSchema()
    WeaponDf.printSchema()
    AREADf.printSchema()
    Victim.printSchema()
    Premis.printSchema()
    Status.printSchema()
    factDf.printSchema()

    //    sampleDataReadWrite(spark, jdbcUrl, jdbcProperties)
    //    val resultDf = factDf.join(loadCrimes,Seq("CrimesId"))
    //    resultDf
    //      .write
    //      .mode("overwrite")
    //      .option("batchsize", "500")
    //      .option("isolationLevel", "NONE")
    //      .jdbc(jdbcUrl, "CrimesCommited", jdbcProperties)
    //      .json("src/main/output/CrimesCommited")

  }

  def sampleDataReadWrite(spark: SparkSession, jdbcUrl: String,jdbcProperties: Properties): Unit = {
        val sampleDf = spark.read
          .option("multiline",true)
          .json("src/main/resources/sample.json")

        sampleDf.show()
        sampleDf.printSchema()

        sampleDf.write.mode("overwrite").jdbc(jdbcUrl,"Sample",jdbcProperties)
//        sampleDf.repartition(1).write.mode("overwrite").json("src/main/output/sample")
  }

  def generateIndex(): Column = {
    val startingIndex = 1
    monotonically_increasing_id() + startingIndex
  }
}