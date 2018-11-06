package com.insight.h1.stats
/**
  * Created by smukherjee5 on 10/30/18.
  */

import scala.collection.Iterator
import scala.io.Source
import java.io._

import com.insight.h1.Helper

object Runner extends Helper{

  def main(args: Array[String]): Unit = {
    val (inputFile, outputOccupationsPath, outputStatesPath) = (args(0), args(1), args(2))
    //val input="/Users/smukherjee5/aws/repos/insight_craft/input/H1B_FY_2014.csv"

    val inputData=Source.fromFile(inputFile).getLines()
    val columns=getColumns(inputData)


    val data=inputData.map(parse).
      filter(getCertifiedApplications(columns,_)).
        map(getProcessorDetails(columns,_))

    //outputOccupations(data,outputOccupationsPath)

  }
  }

//top_10_occupations.txt
//Top 10 occupations,number of certified applications for that occupation (case status certified),%of applications certified compared to total cert irrespective of occupation
//occupation name , for SOC code

case class H1BData(occupationName:String)
//case_status string, employer_name string, soc_name string, job_title string, full_time_position string,prevailing_wage int,year string, worksite string, longitute double, latitute double

case class AggCount(groupByCol: String, count: Int, percent: Double ) {

  def createCSV(sep: String): String = {
    val percentFormatted = "%.1f%%".format(percent)
    s"$groupByCol"+";"+ s"$count"+";"+s"$percentFormatted"
  }}