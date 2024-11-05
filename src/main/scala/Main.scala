package com.tigmaminds.lacrimedataanalysis

import org.apache.spark.sql.{Column, DataFrame, SparkSession}
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

    val LACdf = spark.read
      .option("header",value = true)
      .csv("./src/main/resources/LACrimeData/Crime_Data_from_2020_to_Present.csv")
      .limit(100)

    LACdf.printSchema()

    val CrimeCommitedDf = LACdf
      .select("Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4")
      .dropDuplicates()
      .withColumn("CrimesId",generateIndex())

    val ReportedDf = LACdf.select("Date Rptd", "Rpt Dist No")
      .dropDuplicates()
      .withColumn("ReportedId",generateIndex())

    val OccationDf = LACdf.select("DATE OCC","TIME OCC")
      .dropDuplicates()
      .withColumn("OccationId",generateIndex())

    val Victim = LACdf.select("Vict Age","Vict Sex","Vict Descent")
      .dropDuplicates()
      .withColumn("VictimId",generateIndex())

    val CrimeDf = LACdf.select("Crm Cd","Crm Cd Desc").dropDuplicates()

    val WeaponDf = LACdf.select("Weapon Used Cd", "Weapon Desc").dropDuplicates()

    val AREADf = LACdf.select("AREA", "AREA NAME").dropDuplicates()

    val Premis = LACdf.select("Premis Cd", "Premis Desc").dropDuplicates()

    val Status = LACdf.select("Status", "Status Desc").dropDuplicates()

    val factDf = LACdf.select(
        "DR_NO", "Part 1-2", "Mocodes",
        "LOCATION", "Cross Street", "LAT",
        "LON", "Crm Cd 1", "Crm Cd 2",
        "Crm Cd 3","Crm Cd 4", "Weapon Used Cd",
        "DATE OCC", "TIME OCC", "Date Rptd",
        "Rpt Dist No", "AREA", "Vict Age",
        "Vict Sex","Vict Descent", "Premis Cd", "Status")
      .join(CrimeCommitedDf.select("Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4","CrimesId")
        ,Seq("Crm Cd 1","Crm Cd 2","Crm Cd 3","Crm Cd 4"),"left")
      .join(OccationDf.select("DATE OCC", "TIME OCC","OccationId")
        ,Seq("DATE OCC","TIME OCC"),"left")
      .join(ReportedDf.select("Date Rptd", "Rpt Dist No","ReportedId")
        ,Seq("Date Rptd", "Rpt Dist No"),"left")
      .join(Victim.select("Vict Age","Vict Sex","Vict Descent", "VictimId")
        ,Seq("Vict Age","Vict Sex","Vict Descent"),"left")
      .drop("Crm Cd 1", "DATE OCC", "TIME OCC",
        "Date Rptd", "Rpt Dist No", "Vict Age",
        "Crm Cd 2","Crm Cd 3","Crm Cd 4",
        "Vict Sex","Vict Descent")
      .dropDuplicates()

//    CrimeCommitedDf.printSchema()
//    CrimeDf.printSchema()
//    ReportedDf.printSchema()
//    OccationDf.printSchema()
//    WeaponDf.printSchema()
//    AREADf.printSchema()
//    Victim.printSchema()
//    Premis.printSchema()
//    Status.printSchema()
//    factDf.printSchema()

//    writeToMysql(CrimeCommitedDf,"CrimeCommited")
//    writeToMysql(CrimeDf,"Crime")
//    writeToMysql(ReportedDf,"Reported")
//    writeToMysql(OccationDf,"Occation")
//    writeToMysql(WeaponDf,"Weapon")
//    writeToMysql(AREADf,"AREA")
//    writeToMysql(Victim,"Victim")
//    writeToMysql(Premis,"Premis")
//    writeToMysql(Status,"Status")
//    writeToMysql(factDf,"fact")

      writeAsParquet(CrimeCommitedDf,"CrimeCommited")
      writeAsParquet(CrimeDf,"Crime")
      writeAsParquet(ReportedDf,"Reported")
      writeAsParquet(OccationDf,"Occation")
      writeAsParquet(WeaponDf,"Weapon")
      writeAsParquet(AREADf,"AREA")
      writeAsParquet(Victim,"Victim")
      writeAsParquet(Premis,"Premis")
      writeAsParquet(Status,"Status")
      writeAsParquet(factDf,"fact")

    //    sampleDataReadWrite(spark, jdbcUrl, jdbcProperties)
    //    val resultDf = factDf.join(loadCrimes,Seq("CrimesId"))
    //    resultDf
    //      .write
    //      .mode("overwrite")
    //      .option("batchsize", "500")
    //      .option("isolationLevel", "NONE")
    //      .jdbc(jdbcUrl, "CrimesCommited", jdbcProperties)
    //      .json("src/main/output/CrimesCommited")
    println("Program Terminated successfully")
    spark.close()

  }

  def sampleDataReadWrite(spark: SparkSession, jdbcUrl: String,jdbcProperties: Properties): Unit = {
    val sampleDf = spark.read
      .option("multiline",true)
      .json("src/main/resources/sample.json")

    sampleDf.show()
    sampleDf.printSchema()

    writeToMysql(sampleDf,"sample")
//        sampleDf.repartition(1).write.mode("overwrite").json("src/main/output/sample")
  }

  private def generateIndex(startingIndex: Int = 1): Column = {
    monotonically_increasing_id() + startingIndex
  }

  private def writeToMysql(df: DataFrame, schemaName: String): Unit = {
    val jdbcUrl = "jdbc:mysql://localhost:3306/LACrimes?allowPublicKeyRetrieval=true"
    val jdbcProperties = new java.util.Properties()
    jdbcProperties.setProperty("user", "root")
    jdbcProperties.setProperty("password", "root")
    jdbcProperties.setProperty("driver", "com.mysql.cj.jdbc.Driver")
    df.write.mode("overwrite").jdbc(jdbcUrl,schemaName,jdbcProperties)
  }

  private def writeAsParquet(df: DataFrame, path: String): Unit = {
    df.write.mode("overwrite").parquet("src/main/output/LACrimes/"+path)
  }
}