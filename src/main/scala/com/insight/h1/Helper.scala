package com.insight.h1

import java.io.{BufferedWriter, File, FileWriter, IOException}

import com.insight.h1.stats.{AggCount, H1BData}

import scala.collection.Iterator

/**
  * Created by smukherjee5 on 11/5/18.
  */
class Helper {
  def getColumns(src:Iterator[String]):collection.mutable.Map[String, Int]={
    val hdrLine = src.take(1).next.split(";").map(_.trim)

    val columnMap = collection.mutable.Map[String, Int]()

    for(index <- hdrLine.indices){
      val col = hdrLine(index)
      columnMap.put(col,index)
    }
    columnMap
  }

  def parse(csv:String):Array[String]={
    csv.split(";").map(_.trim)
  }

  def getCertifiedApplications(columnMap:collection.mutable.Map[String, Int],line: Array[String]):Boolean ={
    val isValidApp = (line.apply(columnMap.getOrElse("STATUS",-1))=="CERTIFIED")
    isValidApp
  }

  def getProcessorDetails(columnMap:collection.mutable.Map[String, Int],input:Array[String]):H1BData ={
    H1BData(input.apply(columnMap.getOrElse("LCA_CASE_SOC_NAME",-1)))

  }

  def getField(fieldName: String): Option[String] = {
    fieldName match {
      case "occupationSOC" => Some("LCA_CASE_SOC_NAME")
      //case "workSiteState" => Some(workSiteState)
      case _ => None
    }
  }


  def aggregateCountByOccupations(groupByCol: String, applications: Seq[H1BData] ): Seq[AggCount] = {

    val total = applications.size.toDouble

    val aggregated = applications.groupBy(_.occupationName)
      .mapValues(_.size)
      .map( tup => AggCount(tup._1, tup._2, tup._2/total * 100 ))

    aggregated.toList.
      sortWith(_.groupByCol < _.groupByCol).
      sortWith( _.count > _.count)

  }

  def outputOccupations(data:Seq[H1BData],outFile:String)={
    val out = aggregateCountByOccupations("occupationName",data)
    val header = Array("TOP_OCCUPATIONS","NUMBER_CERTIFIED_APPLICATIONS","PERCENTAGE")
    writeOutCounts( out.take(10), outFile, header)
  }


  def writeOutCounts(aggregated: Seq[AggCount], fileOut: String, header: Array[String]): Unit = {

    var file: File = null
    var bw: BufferedWriter = null

    try {

      file = new File(fileOut)
      bw = new BufferedWriter(new FileWriter(file))

      bw.write(header.mkString(";"))
      bw.newLine()

      for(groupedCount <- aggregated.slice(0,aggregated.length - 1)) {
        bw.write(groupedCount.createCSV(";"))
        bw.newLine()
      }

      //write last line
      bw.write( aggregated.last.createCSV(";") )

    }
    catch {
      case e: IOException => println(s"IOException error, error writing file: $fileOut")
    }
    finally {
      bw.close()
    }

  }


}
