// Databricks notebook source
// MAGIC %md
// MAGIC ## Second Assessment
// MAGIC 
// MAGIC The exam is comprised of four parts that begin with dataset cleansing, analysis, feature engineering, and end with a clustering analysis using SparkML. You can use Scala Spark or PySpark.
// MAGIC 
// MAGIC The objective of this part of the exam is to analyze the raw data, clean it using the strategies we have already seen in class, and create new features.
// MAGIC You will be using the daily COVID-19 reports by the Johns Hopkins University Center for Systems Science and Engineering (JHU CSSE). Thanks to Databricks we don't have to download and import the data, instead we can find it (and a lot more) mounted in the Databricks File System (DBFS).

// COMMAND ----------

// MAGIC %md
// MAGIC We will be using the daily COVID-19 reports that are inside:

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports

// COMMAND ----------

// MAGIC %md Now that we know where the data is, we can proceed with its manipulation and analysis.

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Part 1: Cleaning (20 points)
// MAGIC Create a DataFrame that tabulates the number of confirmed cases, deaths, active and recovered patients, per day per country. Analyze each column to remove unwanted records, typos, and different date formats in the same column (!). Keep only the countries that have at least 30 days of data, and more than 350 confirmed patients on May 3rd, 2020.
// MAGIC 
// MAGIC Some tips:
// MAGIC - There are some incosistencies in the Active column. You should recompute it by evaluating: Active = Confirmed - Deaths - Recovered
// MAGIC - The Country column is named *Country/Region* in some of the CSVs and *Country_Region* in others.
// MAGIC 
// MAGIC For the following parts of the assessment you will be using this DataFrame so make sure it is clean to avoid errors (check for Nulls, empty records, etc.).

// COMMAND ----------

import  org.apache.hadoop.fs.{FileSystem,Path}
import org.apache.hadoop.conf.Configuration
import spark.implicits._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.expressions.Window
import scala.collection.JavaConversions._
import org.apache.spark.sql.functions.typedLit
import org.apache.spark.storage.StorageLevel

// COMMAND ----------

//Function that retuns a list of files in a desiered directory ... 
def listFiles (pathVal: String):Seq[String] = {
  //Create an empty Sequence ...
  var emptySeq = Seq()
  //Saving all paths ... 
  val fs = FileSystem.get(new Configuration())
  val status = fs.listStatus(new Path(pathVal))
  //status.foreach(x=> x.getGroup())
  var pathsSeq = emptySeq :+  status(0).getPath().toString()//Initialize the sequence with the first path 
  //Saving each file path in the sequence ...
  for(p <- status.tail){
     pathsSeq = pathsSeq :+ p.getPath().toString()
  }
  return pathsSeq
}

// COMMAND ----------

//Function that cast a column of a DataFrame to dateType 
def dateParsing(dff: DataFrame):DataFrame = {  
 // Extraction of the column name ... Posible names : Last Update, Last_Update
  var lastUpCol = ""
  for(c <- dff.columns){
    if (c.startsWith("Last")) {
      lastUpCol = c
      //break
    }
  }
 //Parsing the last update column ...
  //println(lastUpCol)
  var finalDf = dff.withColumn("LastUpdateNew", regexp_replace(col(lastUpCol), "/","-"))
                  .withColumn("arrayCol", split($"LastUpdateNew", " "))
                  .withColumn("Date", element_at($"arrayCol", 1))
                  .withColumn("Date",split($"Date", "T"))
                  .withColumn("Date",element_at($"Date", 1))
               
  var dummyDf = finalDf.withColumn("Date", to_date($"Date", "M-d-y")).filter($"Date".startsWith("2")) 
  
 if(dummyDf.isEmpty){
    // If df is empty, then the date format is incorrect 
    dummyDf =  finalDf.withColumn("Date",to_date($"Date","M-d-yy")).filter($"Date".startsWith("2"))
    if(dummyDf.isEmpty){
      // If df is empty, then the date format is incorrect
       return finalDf.withColumn("Date",to_date($"Date","y-M-d"))
    }else{
       return dummyDf
    }
  }else{
    // The dateformat is correct 
    return dummyDf // all observations survived in the filter operation 
  } 
  
}

// COMMAND ----------

//Function in charge of applying a filter to a set of dataFrames 
def filterDf(df2: DataFrame):DataFrame = {
  //Extraction of column name ... Posible names: Country_Region, Country/Region
  var colName = ""
  var len = df2.columns.length
  var count = 0
  while (count <= len){
    colName = df2.columns(count)
    if(colName.startsWith("Country")){
      //break loop
      count = len + 1
    }else{
      count = count + 1 
    }
  } 
  //Filter the disiered columns
  var newDf = df2.select(col(colName),$"Date",$"Confirmed",$"Deaths", $"Recovered")
  //Cast confirmed, deaths and recovered to numerical values  
  //Delete records with no registered country ...
  newDf = newDf.withColumn("Confirmed",$"Confirmed".cast("Double"))
                .withColumn("Deaths",$"Deaths".cast("Double"))
                .withColumn("Recovered",$"Recovered".cast("Double"))
                .withColumn("Country", col(colName))
                .na.drop(Seq("Country"))
  // If and only if the three columns:Confirmed, Deaths and Recovered have null values 
  //then, the row is going to be deleted. 
  newDf = newDf.withColumn("checkThreeVariables", when($"Deaths".isNull && $"Recovered".isNull && $"Confirmed".isNull, true).otherwise(false))
                .filter($"checkThreeVariables" === false)
  // then we impute 0 value in nulls .
  newDf = newDf.withColumn("Deaths", when($"Deaths".isNull,lit(0)).otherwise($"Deaths"))
               .withColumn("Recovered", when($"Recovered".isNull,lit(0)).otherwise($"Recovered"))
               .withColumn("Confirmed", when($"Confirmed".isNull,lit(0)).otherwise($"Confirmed"))
               .withColumn("Active",  $"Confirmed" - ($"Deaths" + $"Recovered") ) 
                         // .otherwise(lit(0)))  //Compute de active cases ...
  
  return newDf.drop(colName,"checkThreeVariables")
}

// COMMAND ----------

//Join all dataFrames
def concatAll(list: Seq[DataFrame]):DataFrame = {  
  var finalFile = list(0)
  for(i <- Range(1,list.length)){
    finalFile = finalFile.union(list(i))  
  } 
  return finalFile
}

// COMMAND ----------

//Function that read all the files in a directory
// and returns a list of dataframes 
def files_to_dfList(p:String, fileType:String, filterType:Boolean):Seq[DataFrame] = {
  var dfSeqDummy = Vector(spark.emptyDataFrame)
  var count = 0
  for (path <- listFiles(p)){
    if(filterType){
      if(path.endsWith(fileType)){
        //reading and saving dataFrames in a sequence ...    
         dfSeqDummy= dfSeqDummy :+ spark.read.format(fileType).option("header",true).load(path)
        //count = count + 1
      }
    }else{
       dfSeqDummy= dfSeqDummy :+ spark.read.format(fileType).option("header",true).load(path)
    }
  }
  return dfSeqDummy
}

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC *Advertencia: La siguiente celda puede tomar cerca de 1hr en correr

// COMMAND ----------

//Reding all csv files 
var dfList = files_to_dfList("/databricks-datasets/COVID/CSSEGISandData/csse_covid_19_data/csse_covid_19_daily_reports", "csv",true)
dfList= dfList.tail //delete the first element (it was just an empty SPARK DF)
//casting the last update column to dateType for each dataFrame
var counter = 0
for(dataf <- dfList){
  var cleanDf = dateParsing(dataf) //Casting
  dfList = dfList.updated(counter,filterDf(cleanDf)) //drop null values 
  counter = counter + 1
}

// COMMAND ----------

//Concatenate all dataFrames 
var allDf = concatAll(dfList)
//display(allDf)

// COMMAND ----------

//GroupBy date and Country
var aggDf = allDf.groupBy("Date","Country")
                  .agg(
                    sum("Confirmed").as("Confirmed"),
                    sum("Deaths").as("Deaths"),
                    sum("Recovered").as("Recovered"),
                    sum("Active").as("Active")
                  )
//Count the number of records per Country 
var dfCount = aggDf.groupBy($"Country".as("Country_count")).count().filter($"count" >= 30) 
//Keep contries with at least 30 records
var joinedDf = aggDf.join(dfCount, aggDf("Country") === dfCount("Country_count"), "inner")
//Keep the contries that have more than 350 confirmed cases on 2020-05-03
var cleanedResults =  joinedDf.filter($"Date" === "2020-05-03" && $"Confirmed" > 350).select($"Country_count",$"Confirmed".as("Confirmed_filtered"))
var cleanedJoinedDf = cleanedResults.join(joinedDf,cleanedResults("Country_count") === joinedDf("Country"), "inner" )
cleanedJoinedDf = cleanedJoinedDf.select("Date","Country","Confirmed","Deaths","Recovered","Active")

// COMMAND ----------

//Write Df in a parquet file 
cleanedJoinedDf.write
      .format("parquet")
      .mode("overwrite")
      .option("header",true)
      .save("/FileStore/cleanCovidNewActiveComputation")

// COMMAND ----------

// MAGIC %md
// MAGIC **Showing Clean Data Frame: **

// COMMAND ----------

//Red clean file
var cleanCovidDf= spark.read.format("parquet") // Define a format
                                .option("header", true) // Headers?
                                .load("/FileStore/cleanCovidFinal")
display(cleanCovidDf)    

// COMMAND ----------

// MAGIC %md 
// MAGIC ####Part 2: Analyze (20 points)
// MAGIC 
// MAGIC #####2.a (5 points)
// MAGIC 
// MAGIC Using the most recent data, plot the top-5 countries and bottom-5 countries (in the same dataframe) for each: Active, Confirmed, Deaths, Recovered. This will result in a total of 4 plots.
// MAGIC 
// MAGIC #####2.b (15 points)
// MAGIC 
// MAGIC Using the most recent data (beware, the file is updated regularly) plot the number of death, active, recovered, and confirmed cases using the "World Map" utility. This will result in a total of 4 maps.
// MAGIC 
// MAGIC **Note:** To use the "World Map" display tool in Databricks you'll need to use the ISO 3166-1 alpha-3 naming convention for the countries; if you can't find the code for a country, skip it only for this point. You can use the python library iso3166 (you'll need to install it in your cluster) to convert the names automatically. Hint: this is a python function, therefore using your knowledge about UDF could be a good idea.

// COMMAND ----------

// Find most recent date
var last_date = cleanCovidDf.select(max("Date")).alias("date")
var last_data= cleanCovidDf.filter($"Date" === last_date.first.toString().replaceAll("[\\[\\]]",""))
display(last_data)

// COMMAND ----------

//Active plot
var top5_active = last_data.select("Country","Active")
                 .orderBy(desc("Active"))
                 .limit(5)
var bottom5_active = last_data.select("Country","Active")
                 .orderBy(asc("Active"))
                 .limit(5)
display(top5_active.unionAll(bottom5_active))

// COMMAND ----------

//Confirmed plot
var top5_confirmed = last_data.select("Country","Confirmed")
                 .orderBy(desc("Confirmed"))
                 .limit(5)
var bottom5_confirmed = last_data.select("Country","Confirmed")
                 .orderBy(asc("Confirmed"))
                 .limit(5)
display(top5_confirmed.unionAll(bottom5_confirmed))

// COMMAND ----------

//Deaths plot
var top5_deaths = last_data.select("Country","Deaths")
                 .orderBy(desc("Deaths"))
                 .limit(5)
var bottom5_deaths = last_data.select("Country","Deaths")
                 .orderBy(asc("Deaths"))
                 .limit(5)
display(top5_deaths.unionAll(bottom5_deaths))

// COMMAND ----------

//Recovered
var top5_recovered = last_data.select("Country","Recovered")
                 .orderBy(desc("Recovered"))
                 .limit(5)
var bottom5_recovered = last_data.select("Country","Recovered")
                 .orderBy(asc("Recovered"))
                 .limit(5)
display(top5_recovered.unionAll(bottom5_recovered))

// COMMAND ----------

// MAGIC %python
// MAGIC %pip install -U dataprep==0.3.0a0
// MAGIC ## Decidí usar esa librería en vez de la otra

// COMMAND ----------

last_data.createOrReplaceTempView("pd_lastdata")

// COMMAND ----------

// MAGIC %python
// MAGIC import pyspark.sql.functions as F
// MAGIC import pyspark.sql.types as T
// MAGIC from pyspark.sql.functions import pandas_udf, PandasUDFType
// MAGIC 
// MAGIC from dataprep.clean import clean_country
// MAGIC import pandas as pd
// MAGIC 
// MAGIC ##from iso3166 import countries
// MAGIC 
// MAGIC lastdata_udf = spark.table("pd_lastdata")
// MAGIC 
// MAGIC @pandas_udf("Date date, Country string, Confirmed long, Deaths long, Recovered long, Active long, Country_clean string", PandasUDFType.GROUPED_MAP)
// MAGIC def country_conversion(pdf):
// MAGIC   ######################################################
// MAGIC   # Write the UDF code here, return a pandas dataframe #
// MAGIC   pdf = clean_country(pdf, 'Country', output_format='alpha-3')
// MAGIC   ######################################################
// MAGIC   return pdf
// MAGIC 
// MAGIC # Get the coefficients for the train set
// MAGIC lastdata_iso = lastdata_udf.groupBy().apply(country_conversion)
// MAGIC lastdata_iso.registerTempTable("lastdata_iso_udf")

// COMMAND ----------

var lastdata_iso_table = table("lastdata_iso_udf")

// Active World Map
display(lastdata_iso_table)

// COMMAND ----------

// Confirmed World Map
display(lastdata_iso_table)

// COMMAND ----------

// Deaths World Map
display(lastdata_iso_table)

// COMMAND ----------

// Recovered World Map
display(lastdata_iso_table)

// COMMAND ----------

// MAGIC %md 
// MAGIC #### Part 3: Feature Engineering (35 points)
// MAGIC 
// MAGIC ##### 3.a (15 points)
// MAGIC 
// MAGIC Create a new DataFrame in which each country is a row and it has a column for each of the following features: 
// MAGIC   1. The day in which the first confirmed case appeared.
// MAGIC   2. No. of days from 1 to 10 confirmed cases.
// MAGIC   3. No. of days from 1 to 100 confirmed cases.
// MAGIC   4. No. of days from 1 to 5,000 confirmed cases
// MAGIC   5. No. of days from 5,000 to 10,000 confirmed cases.
// MAGIC   6. No. of days from 10,000 to 50,000 confirmed cases.
// MAGIC   7. No. of days from 50,000 to 100,000 confirmed cases.
// MAGIC   8. No. of days from 100,000 to 500,000 confirmed cases.
// MAGIC 
// MAGIC ##### 3.b (15 points)
// MAGIC 
// MAGIC For each country, compute its Total Absolute Difference and Total Relative Difference vs Mexico's features 2 to 8, and plot the results. If a feature value is Null (because of low number of cases) then drop the Country from this analysis. To compute the difference between Mexico and country B, do the following:
// MAGIC 
// MAGIC | Country             | Feature 2 | Feature 3 | Feature 4 | Feature 5 | Feature 6 | Feature 7 | Feature 8 |
// MAGIC |---------------------:|:-----------:|:-----------:|:-----------:|:-----------:|:-----------:|:-----------:|:-----------:|
// MAGIC | Country B           | 10        | 4         | 1         | 9         | 8         | 8         | 10        |
// MAGIC | Mexico              | 3         | 6         | 2         | 2         | 8         | 10        | 7         |
// MAGIC | Relative Difference | 7         | -2        | -1        | 7         | 0         | -2        | 3         |
// MAGIC | Absolute Difference | 7         | 2         | 1         | 7         | 0         | 2         | 3         |
// MAGIC 
// MAGIC In the above case, the Total Absolute Difference between Mexico and B is the sum of Absolute Differences, 22. 
// MAGIC The Total Relative Difference between Mexico and B is the sum of Relative Differences, 12. 
// MAGIC 
// MAGIC ##### 3.c (5 points)
// MAGIC 
// MAGIC Show the 5 most similar countries to Mexico, and the 5 most different by Total Absolute Difference. Do the same for the Total Relative Difference. Are the most similar countries to Mexico in those features, still near the number of deaths/active/confirmed/recovered that is shown for the most recent data?

// COMMAND ----------

//Delete the records where Confirmed Cases = 0 
//Just Sudan and Iraq have reported 0 confirmed cases in their first recorded day 
//(Therefore, just two rows are going to be deleted)
var featureEng = cleanCovidDf.filter($"Confirmed" > 0 ).orderBy($"Date")
//Pesists experiment ... 
val featureEngCache = featureEng.persist(StorageLevel.MEMORY_AND_DISK)
//display(featureEngCache)


// COMMAND ----------

//Encontrar el limite inferior y superior uno de los paises ... 
//Function that return the lower and upper limits of each country  
// i confirmed case to the k case, where k and i are positive integers 
def upperLowerLimits(featureFuncDf:DataFrame, tupla: Vector[(Int,Int)] ):DataFrame = {
  
 // for (limits <- tupla){
    var upper = tupla(0)._2
    var lower = tupla(0)._1
    var subStr_0 = lower.toString() + "_" + upper.toString()
    var originalDf = featureFuncDf.filter($"Confirmed" >= lower).orderBy($"Date")
    //Considering only  the first appearance of the upper limit  
    val windowF = Window.partitionBy($"UpperLimit").orderBy($"Date")
    val w2 = Window.partitionBy($"Country").orderBy($"Date")
    var finalLimits = originalDf.withColumn("UpperLimit", when($"Confirmed" >= upper, true).otherwise(false)) //Identifying the cases greater or equal to the upper limit 
                              .withColumn("RankingUpper", row_number().over(windowF))
                              .filter($"RankingUpper" === 1) 
                              .withColumn("LowerLimit_"+subStr_0, $"Confirmed")
                              .withColumn("UpperLimit_"+subStr_0, lag($"Confirmed",-1).over(w2)).drop("UpperLimit","RankingUpper").limit(1)
 // }
  
  for(limits <- tupla.tail){
    
    var subStr = limits._1.toString() + "_" + limits._2.toString()
    var key = "Country"+ subStr
     
     var originalDf = featureFuncDf.filter($"Confirmed" >= limits._1).orderBy($"Date")
    //Considering only  the first appearance of the upper limit  
   // val windowF = Window.partitionBy($"UpperLimit").orderBy($"Date")
    val w2 = Window.partitionBy($"Country").orderBy($"Date")
    var finalLimits2 = originalDf.withColumn("UpperLimit", when($"Confirmed" >= limits._2, true).otherwise(false)) //Identifying the cases greater or equal to the upper limit 
                              .withColumn("RankingUpper", row_number().over(windowF))
                              .filter($"RankingUpper" === 1) 
                              .withColumn("LowerLimit_"+subStr, $"Confirmed")
                              .withColumn("UpperLimit_"+subStr, lag($"Confirmed",-1).over(w2))  
                              .withColumn(key, $"Country").drop("Country").limit(1).drop("UpperLimit","RankingUpper")
    
    finalLimits = finalLimits.join(finalLimits2,finalLimits("Country") === finalLimits2(key) , "inner" ).drop(key)
  }
  
  return finalLimits.drop("Date","Confirmed","Deaths","Recovered","Active","Confirmed")//.select("Country","LowerLimit_"+i.toString() + "_" + k.toString(),"UpperLimit_"+i.toString() + "_" + k.toString())
}

// COMMAND ----------

//Extraction of the first day that a confirmed case was reported
//slice() function
//
var finalFeatureEgDf = featureEngCache.groupBy($"Country").agg(first($"Date").as("FirstDay"), collect_list("Confirmed").as("Confirmed"))
var finalFeatureEgDfCahe = finalFeatureEgDf.persist(StorageLevel.MEMORY_AND_DISK)
//display(finalFeatureEgDfCahe)

// COMMAND ----------

// MAGIC %md
// MAGIC Advertencia: La siguiente celda puede tardar más de 1hr en terminar de correr

// COMMAND ----------

//Computing the number of days for a specific number of confirmed cases for each country
//Just the countries that have at leats 7 variables will survive
var countryList = finalFeatureEgDfCahe.select("Country").rdd.map(r => r(0)).collect() //List of country names
var casesIk = Vector((1,10),(1,100),(1,5000),(5000,10000),(10000,50000),(50000,100000),(100000,500000))
var init_0 = 0
var initialCounter1 = init_0 + 1
var initialCounter2 = initialCounter1 + 6

for(i <- Range(0,countryList.length/7)){

  //First country
  var tranformDF = featureEngCache.filter($"Country" === countryList(init_0))
  var limitsStorage = upperLowerLimits(tranformDF,casesIk)
  //Rest of the countries

  for(i <- Range(initialCounter1,initialCounter2)){
    tranformDF = featureEngCache.filter($"Country" === countryList(i))
    var functionOfLimits = upperLowerLimits(tranformDF,casesIk)
    limitsStorage = limitsStorage.union(functionOfLimits)
  }
  //Save the current computations 
  var newPathName = init_0.toString() +"_"+ (initialCounter2 - 1).toString()
  limitsStorage.write
      .format("parquet")
      .mode("overwrite")
      .option("header",true)
      .save("/FileStore/limitsStorage_" + newPathName)
 //Update counter
 init_0 = init_0 + 7
 initialCounter1 = init_0 + 1 
 initialCounter2 = initialCounter1 + 6 
}

// COMMAND ----------

//Reading and concatenete each file 
var limitsDfList = files_to_dfList("/FileStore/test","parquet",false).tail
var limitsConcat = concatAll(limitsDfList).withColumn("CountryConcat", $"Country").drop($"Country")
//Join
var newLimitsDf = finalFeatureEgDfCahe.join(limitsConcat,finalFeatureEgDfCahe("Country") ===limitsConcat("CountryConcat"),"inner")
                                      .drop($"CountryConcat")
                                      

// COMMAND ----------

var casesIk2 = Vector((1,10),(1,100),(1,5000),(5000,10000),(10000,50000),(50000,100000),(100000,500000))
var lowerUpperLabels = Vector("LowerLimit","UpperLimit")
for(cases2 <- casesIk2){
  var subStr2 = "_" +  cases2._1.toString() + "_" + cases2._2.toString()
  for(limitName <- lowerUpperLabels){
    newLimitsDf = newLimitsDf.withColumn(limitName + "Index" + subStr2,  array_position($"Confirmed",col(limitName + subStr2)))
  }
  var subStr3 = "_" +  cases2._1.toString() + "_to_" + cases2._2.toString()
  newLimitsDf = newLimitsDf.withColumn("#Days" + subStr3 + "cases", col("UpperLimitIndex" +subStr2) - col("LowerLimitIndex" + subStr2 )+ 1)
                            .drop("UpperLimitIndex" +subStr2,"LowerLimitIndex" + subStr2  )

}

// COMMAND ----------

// MAGIC %md
// MAGIC ** 3.a New Data Frame:**

// COMMAND ----------

//Select the useful columns and deleting countries with null values
var newLimitsDfFiltered =  newLimitsDf.select("Country","FirstDay","#Days_1_to_10cases","#Days_1_to_100cases","#Days_1_to_5000cases",
                                             "#Days_5000_to_10000cases","#Days_10000_to_50000cases","#Days_50000_to_100000cases","#Days_100000_to_500000cases").na.drop()
display(newLimitsDfFiltered)

// COMMAND ----------

//Function to transpose a dataFrame
def transposeDf(df:DataFrame, schemaCol: String):DataFrame = {
  //First column must be the new schema 
  val new_schema = StructType(newLimitsDfFiltered.select(collect_list(schemaCol)).first().getAs[Seq[String]](0).map(z => StructField(z, StringType)))
  var cols = df.columns.tail
  val numberOfRows = cols.length - 1 
  var rows = Seq(Row.fromSeq(newLimitsDfFiltered.select(col(cols(0)).cast("String")).rdd.map(r => r(0)).collect()))
  var count = 1
  //Store the rows in a vector 
  for(colName <- cols.tail){
    rows = rows :+ Row.fromSeq(newLimitsDfFiltered.select(col(colName).cast("String")).rdd.map(r => r(0)).collect())
    
  }
  //result
  return spark.createDataFrame(rows, new_schema)
  
}

// COMMAND ----------

// MAGIC %md
// MAGIC **3.b Computing the total absolute difference and total relative difference**

// COMMAND ----------

var diffDfList = Vector(spark.emptyDataFrame) //List of total differences dataFrames
var transposedDataFrame =  transposeDf(newLimitsDfFiltered.drop("FirstDay"), "Country")
var colsTransposed = transposedDataFrame.columns
//Compute the relative and absolute differences for each country VS Mexico
for(country <- colsTransposed){
  //Save the computed differences for each country in a vector
  if (country != "Mexico"){
     diffDfList = diffDfList :+ transposedDataFrame.withColumn("diff_" + country + "_Mexico", col(country) - $"Mexico" )
                                                  .withColumn("abs_diff_"+ country + "_Mexico", abs(col("diff_"+ country +"_Mexico")))
                                                  .withColumn("dummyCol", lit(1))
                                                  .groupBy("dummyCol")
                                                  .agg(sum("abs_diff_"+country+"_Mexico").as("Absolute_total_difference"),
                                                  sum("diff_"+country+"_Mexico").as("Relative_total_difference"))
                                                  .withColumn("Comparison (Country vs Mexico)", lit(country))
                                                  .drop("dummyCol") 
  }
}
//Union of the previous computed dataFrames 
var tailList = diffDfList.tail
var differencesDf = tailList(0)
for(table <-  tailList.tail){
  differencesDf = differencesDf.union(table)
}

display(differencesDf)

// COMMAND ----------

//Ploting the results 
display(differencesDf)

// COMMAND ----------

// MAGIC %md
// MAGIC **3.c Five most similar countries  to Mexico, and five most different  (Using the Absolute total difference and the Relative total difference)**

// COMMAND ----------

//Show the 5 most similar countries to Mexico, and the 5 most different by Total Absolute Difference. Do the same for the Total Relative Difference. Are the most similar countries to Mexico in those features, still near the //number of deaths/active/confirmed/recovered that is shown for the most recent data?

//Computing the 5 most similar countries using the Absolute_total_difference (the countrie with the lower value is the most similar to Mexico)
var mostSimilar = differencesDf.orderBy("Absolute_total_difference")
                     .limit(5).withColumn("Category", lit("Most similar"))
//Showing the results
display(mostSimilar.select("Comparison (Country vs Mexico)","Absolute_total_difference"))

// COMMAND ----------

//Computing the 5 most different countries using the Absolute_total_difference (the countrie with the higher value is the most similar to Mexico)
var mostDifferent = differencesDf.orderBy(desc("Absolute_total_difference"))
                                 .limit(5).withColumn("Caegory", lit("Most different"))                                 
//Showing the results
display(mostDifferent.select("Comparison (Country vs Mexico)","Absolute_total_difference"))

// COMMAND ----------

//Visualizing the results (computed with absulute difference)
var unionOfData = mostSimilar.union(mostDifferent)
display(unionOfData)

// COMMAND ----------

//Computing the 5 most similar countries using the Relative_total_difference (the countrie with the closest value to  zero  is the most similar to Mexico)

var mostSimilarRelative = differencesDf.withColumn("RelativeAllPositiveDummy", abs($"Relative_total_difference"))
                     .orderBy("RelativeAllPositiveDummy")
                     .limit(5).withColumn("Caegory", lit("Most similar")) 
//Showing the results
display(mostSimilarRelative.select("Comparison (Country vs Mexico)","Relative_total_difference"))

// COMMAND ----------

//Computing the 5 most similar countries using the Relative_total_difference (the countrie witht the value furthest from zero  is the most different to Mexico)

var mostDifferentRelative = differencesDf.withColumn("RelativeAllPositiveDummy", abs($"Relative_total_difference"))
                     .orderBy(desc("RelativeAllPositiveDummy"))
                     .limit(5).withColumn("Caegory", lit("Most diffrent")) 
//Showing the results
display(mostDifferentRelative.select("Comparison (Country vs Mexico)","Relative_total_difference"))

// COMMAND ----------

//Visualizing the results (computed with relative difference)
var unionOfData2 = mostSimilarRelative.union(mostDifferentRelative)
display(unionOfData2)

// COMMAND ----------

// MAGIC %md
// MAGIC **Are the most similar countries to Mexico in those features, still near the number of deaths/active/confirmed/recovered that is shown for the most recent data?**

// COMMAND ----------

// MAGIC %md
// MAGIC *Analysis of Deaths and Confirmed cases curves*

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" || $"Country" === "Peru").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" || $"Country" === "Colombia").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" ||  $"Country" === "Argentina").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" ||  $"Country" === "South Africa").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" || $"Country" === "Spain").orderBy("Country","Date"))

// COMMAND ----------

// MAGIC %md
// MAGIC In the previous graphs, we can see that the countries that are still close to the number of cases reported in Mexico (up to the most recent date recorded in the database) are Colombia and Argentina. The curve of confirmed cases for the remaining three countries is above or below that of Mexico in the most recent dates. On the other hand, the death numbers of the 5 countries analyzed are not similar to those reported by Mexico recently, in the 5 cases Mexico has the highest number of samples (until March 12, 2021)

// COMMAND ----------

// MAGIC %md
// MAGIC *Analysis of active and recovered curves*

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" || $"Country" === "Peru").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" || $"Country" === "Colombia").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" ||  $"Country" === "Argentina").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" ||  $"Country" === "South Africa").orderBy("Country","Date"))

// COMMAND ----------

display(cleanCovidDf.filter($"Country" === "Mexico" || $"Country" === "Spain").orderBy("Country","Date"))

// COMMAND ----------

// MAGIC %md
// MAGIC According to the previous figures, Argentina is the only country with the number of recovered and active cases most similar to Mexico still in the most recent data. The other countries no longer have similarity in this curves (for the most recent data recorded in the data base)

// COMMAND ----------

// MAGIC %md #### Part 4: K-means (25 points)
// MAGIC 
// MAGIC **Objective:** Run a [SparkML K-means](https://spark.apache.org/docs/latest/ml-clustering.html#k-means) algorithm using the features 2 to 8 from the first part of the exam and plot the results.
// MAGIC 
// MAGIC Follow these steps:
// MAGIC 
// MAGIC **4.a (5 points): ** Create a K-Means model setting the number of clusters (_k_) equal to 5. 
// MAGIC 
// MAGIC **4.b (2 points): ** Create a plot showing your results (be creative).
// MAGIC 
// MAGIC **4.c (16 points): ** Use (if you want to) the code in the next cell to run an "elbow test" to define an optimal number of clusters. To do this, you need to plot the cost for each *k* value and use the results to select the best k.
// MAGIC 
// MAGIC **4.d (2 points): ** Create a plot showing the new results. How are they different from those in second point?

// COMMAND ----------

display(newLimitsDfFiltered)

// COMMAND ----------

import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.evaluation.ClusteringEvaluator


val columns = Array("#Days_1_to_10cases","#Days_1_to_100cases","#Days_1_to_5000cases",
                    "#Days_5000_to_10000cases","#Days_10000_to_50000cases","#Days_50000_to_100000cases","#Days_100000_to_500000cases")

val assembler = new VectorAssembler().setInputCols(columns).setOutputCol("features")
val feature_df = assembler.transform(newLimitsDfFiltered)

// COMMAND ----------

val Array(train_df, test_df) = feature_df.randomSplit(Array(.8, .2), seed=42)

// COMMAND ----------

val kmeans = new KMeans()
  .setK(5)
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
val kmeansModel = kmeans.fit(train_df)
display(kmeansModel,feature_df)

// COMMAND ----------

// Make predictions
val predictDf = kmeansModel.transform(test_df)
display(predictDf)

// COMMAND ----------

// Silhouette score
val evaluator = new ClusteringEvaluator()

val silhouette = evaluator.evaluate(predictDf)
println(s"Silhouette with squared euclidean distance = $silhouette")

// COMMAND ----------


var cost : Array[Double] = Array()

for( k <- 2 to 10)
  {val km = new KMeans() // Kmeans object
              .setK(k) // Define the k hyperparameter
              .setFeaturesCol("features") // Name of the column containing train features
              .setPredictionCol("prediction") // Name of the column where the predictions will be placed

  val kmModel = km.fit(train_df) // Fit model
  val predictions = kmModel.transform(test_df)
   
  val evaluator = new ClusteringEvaluator()
  cost :+= evaluator.evaluate(predictions)
  }

// COMMAND ----------

val new_kmeans = new KMeans()
  .setK(3)
  .setFeaturesCol("features")
  .setPredictionCol("prediction")
val new_kmeansModel = new_kmeans.fit(train_df)

display(new_kmeansModel,feature_df)

// COMMAND ----------

display(new_kmeansModel,feature_df)

// COMMAND ----------

val new_predictDf = new_kmeansModel.transform(test_df)
display(new_predictDf)

// COMMAND ----------

// Silhouette score
val evaluator = new ClusteringEvaluator()

val new_silhouette = evaluator.evaluate(new_predictDf)
println(s"Silhouette with squared euclidean distance = $new_silhouette")

// COMMAND ----------

// MAGIC %md #### Extra points: Parametric Dashboard (5 points)
// MAGIC 
// MAGIC In another notebook and using the same COVID-19 dataset, create a dashboard with at least 5 plots or insights that can be useful to track. You are free to include any widgets you want, but it must have at least a "date" dropdown widget that updates the contents of the dashboard according to its value. The 1 best dashboards will get an extra 5 points (resulting in a total of 10 extra points).

// COMMAND ----------

