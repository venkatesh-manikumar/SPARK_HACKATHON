package org.inceptez.sparkusecases

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions.{concat,col,lit,udf,max,min,current_date,current_timestamp,regexp_replace,column,row_number}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType ,DateType}
import org.inceptez.hack.allmethods
import java.text.SimpleDateFormat
import java.util.Date
import java.util.Properties

object spark_core_usecase 
{
  
  case class Insureclass(IssuerId : Int,	
                         IssuerId2 : Int,
                         BusinessDate : String,
                         StateCode : String,	
                         SourceName : String,	
                         NetworkName : String,	
                         NetworkURL : String,
                         custnum	: Int,
                         MarketCoverage : String,
                         DentalOnlyPlan : String)

  
  def main(args: Array[String]) {
   
    
    val sparkSession=SparkSession.builder().appName("HACKATHON")
                                      .master("local[*]")
                                      .config("spark.history.fs.logDirectory","file:///tmp/spark-events")
                                      .config("spark.eventLog.dir","file:///tmp/spark-events")
                                      .config("spark.eventLog.enabled","true")
                                      .config("hive.metastore.uris","thrift://localhost:9083")
                                      .config("spark.sql.warehouse.dir","hdfs://localhost:54310/user/hive/warehouse")
                                      .enableHiveSupport().getOrCreate();
    
    
    
     
    val sc= sparkSession.sparkContext
    sc.setLogLevel("ERROR")
    val sqlc=sparkSession.sqlContext
    
    
   import sqlc.implicits._
   
   println("1.Load file from HDFS using textfile API into RDD")
   /*NEED CHANGES*/
 
    /* /home/hduser/workspacespark/New_workspace/Spark_core_usecsses/src/org/inceptez/sparkusecases/insuranceinfo1.csv
     * hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo1.csv
     * 
     */
   val insuredata= sc.textFile("/home/hduser/workspacespark/New_workspace/Spark_core_usecsses/src/org/inceptez/sparkusecases/insuranceinfo1.csv")
   insuredata.cache()
   
   println("2.Removing Header from RDD containing column names")
   val header = insuredata.first()
   println(s"Header record => ${header}")
   val data = insuredata.filter(row => row!=header)
   
   
   
   println("3.Display count and show few rows and check whether header is removed")
   println(s"Record count after removing header => ${data.count()}")
   println("Record sample")
   data.take(5).foreach(println)
   
   
   println("4.Remove blank lines from RDD")
   val res=data.filter(row=> row.trim().length() > 0)
   
   /*
   VALIDATION
   res.map(i=> i.trim().length() == 0).take(10).foreach(println)
   * 
   */
   
   println("5.Map and split using "," delimiter")
   val Z1=res.map(r=> r.split(",",-1)).map(x=> (x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9)))
   
 
   val insure=res.map(dat=> {
     (dat.split(",",-1)(0),
      dat.split(",",-1)(1),
      dat.split(",",-1)(2),
      dat.split(",",-1)(3),
      dat.split(",",-1)(4),
      dat.split(",",-1)(5),
      dat.split(",",-1)(6),
      dat.split(",",-1)(7),
      dat.split(",",-1)(8),
      dat.split(",",-1)(9))
      
    })
    
  
    
     /*  VALIDATION
      *  insure.take(10).foreach(println)
     */
     
      
    println("6.Filter number of fields equal to 10")
    val t1=insure.filter(i=> i.productIterator.length==10 && 
                           ( i._1.trim()!="" && 
                             i._2.trim()!="" && 
                             i._3.trim()!="" && 
                             i._4.trim()!="" && 
                             i._5.trim()!="" && 
                             i._6.trim()!="" && 
                             i._7.trim()!="" &&
                             i._8.trim()!="" ))
                       /* && i._9.trim()!="" &&
                             i._10.trim()!=""
                           */ 
    
    t1.take(10).foreach(println)  
    
    def convertDate(d:String,format:String):String={
      
      if (format.equals("yyyy-MM-dd"))   
      {
        val inputformat=new SimpleDateFormat("yyyy-MM-dd")
        val outputformat=new SimpleDateFormat("yyyy-MM-dd")
        inputformat.parse(d)
        outputformat.format(inputformat.parse(d))
        }
    
      else if (format.equals("dd-MM-yyyy")) 
      {
        val inputformat=new SimpleDateFormat("dd-MM-yyyy")
        val outputformat=new SimpleDateFormat("yyyy-MM-dd")
        inputformat.parse(d)
        outputformat.format(inputformat.parse(d))
      }
      
      else 
        return "Format mismatch";
  
    }
    println("7.Add case class -insureclass with fields as per Header record and create a schemaed RDD")
    val t2=t1.map(x=>Insureclass(x._1.toInt,x._2.toInt,convertDate(x._3,"yyyy-MM-dd"),x._4,x._5,x._6,x._7,x._8.toInt,x._9,x._10))
      
    /*
    VALIDATION
    t2.filter(i=> i.IssuerId.toString().trim().length()==0).take(10).foreach(println)
    */ 
      
    t2.take(10).foreach(println)

      
      println("8.Take the count of RDD created in step 7 and step 1")
      println(s"RAW DATA COUNT - STEP 01 => ${insuredata.count()}")
      println(s"PROCESSED DATA COUNT - STEP07 => ${t2.count()}")
      println(s"No of records dropped in pre-processing => ${insuredata.count()-t2.count()}")
   
      
      println("9.Create another RDD named rejectdata, store rows that doesnot equals 10 columns")
      
      /* VALIDATION
       * val rejectdata=insure.filter(i=> i.productIterator.length != 10)
       */
      
      println("Rejected Records")
      val rejectdata=insure.subtract(t1)
      rejectdata.take(10).foreach(println)
      rejectdata.count()
      
      
      println("10.Load file2 (insurancedata2) from HDFS")
      val insurancedata2= sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/insuranceinfo2.csv")
      insurancedata2.cache()

      println("11.Repeat from step 2 to 9 for this file also and create the final rdd including the filtering of the records that contains blank IssuerId,IssuerId2")
      val head=insurancedata2.first()
      
      /*Header removed*/
      val p1=insurancedata2.filter(row => row != head)
      
      /*Blank lines  removed*/
      val p2=p1.filter(row=> row.trim().length() > 0)
       
      /*Including incomplete data*/
      p2.take(10).foreach(println)
      val p3=p2.map(dat=> {
         (dat.split(",",-1)(0).trim(),
          dat.split(",",-1)(1).trim(),
          dat.split(",",-1)(2).trim(),
          dat.split(",",-1)(3).trim(),
          dat.split(",",-1)(4).trim(),
          dat.split(",",-1)(5).trim(),
          dat.split(",",-1)(6).trim(),
          dat.split(",",-1)(7).trim(),
          dat.split(",",-1)(8).trim(),
          dat.split(",",-1)(9).trim()
          
         )
      })
      
      /*Filters incomplete records*/
      val p4=p3.filter(i=> i.productIterator.length==10 && 
                          (i._1.trim()!="" && 
                           i._2.trim()!="" && 
                           i._3.trim()!="" && 
                           i._4.trim()!="" && 
                           i._5.trim()!="" && 
                           i._6.trim()!="" && 
                           i._7.trim()!="" && 
                           i._8.trim()!=""))
      
      p4.take(10).foreach(println)
      
      
      val p5=p4.map(x=>Insureclass(x._1.toInt,x._2.toInt,convertDate(x._3,"dd-MM-yyyy"),x._4,x._5,x._6,x._7,x._8.toInt,x._9,x._10))
      
      println("***VALIDATION****")
      println(s"Raw Data count => ${p1.count()}")
      println(s"Processed Data count => ${p5.count()}")
      println(s"No of records dropped in pre-processing => ${p1.count()-p5.count()}")
      p5.take(10).foreach(println)
      
      println("REJECTED RECORDS")
      println(p3.subtract(p4).count())
      val rejectdata_1=p3.subtract(p4)
      p3.subtract(p4).take(5).foreach(println)
      
      
      /* VALIDATION
       * println("TESTING 1")                  
      p4.take(403).foreach(println)
      p4.count()*/
      
      /*val p5=p4.map(p=> p.IssuerId == "")
      println("TESTING 2")
      p5.take(10).foreach(println)*/
                           
      println("12.Merge both header removed RDD from step7 and step 11 into new RDD insuredatamerged")                        
      val insuredatamerged=t2.union(p5)
      
      println("13.Cache it to memory")
      insuredatamerged.cache()
      

      println("14.Calculate count of RDD created in STEP07 + STEP11")
      println(s"RDD record count created in STEP 07 ${t2.count()}")
      println(s"RDD record count created in STEP 11 ${p5.count()}")
      println(s"RDD record count created in STEP 07+11 ${t2.count()+p5.count()}")
      println(s"RDD record count created in STEP 12 ${insuredatamerged.count()}")
      
      
      println("15.Remove duplicates from mergerd RDD created in step12")
      val total =insuredatamerged.count()
      val dedup_data=insuredatamerged.distinct()
      val dedup_count=insuredatamerged.distinct().count()
      val dup_count = total-dedup_count
      
      println(s"Total count ${total}")
      println(s"Dedup count ${dedup_count}")
      println(s"Duplicate count ${dup_count}")
    

      println("16.Increase number of partitions to 8 and name it as insuredatarepart")
      val insuredatarepart = dedup_data.repartition(8)
      println(s"insuredatarepart total count => ${insuredatarepart.count()}")
      println(s"Partitions size ${insuredatarepart.partitions.size}")
      println(s"Partition glom collect ${insuredatarepart.glom().collect}")

      
      /*
      VALIDATION
      insuredatarepart.map(i=> i.BusinessDate).distinct().take(50).foreach { println }
      println(insuredatarepart.filter { x => x.BusinessDate.trim() == "2019-10-01" }.count())
      println(insuredatarepart.filter { x => x.BusinessDate.trim() == "2019-10-02" }.count())
      
      * 
      */
      
      println("17. Split the above RDD using the businessdate field into rdd_20191001 and rdd_20191002 based on the BusinessDate of 2019-10-01 and 2019-10-02 respectively")
      insuredatarepart.filter { x => x.BusinessDate.trim() == "2019-10-01" }.coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step17/rdd_20191001.txt")
      insuredatarepart.filter { x => x.BusinessDate.trim() == "2019-10-02" }.coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step17/rdd_20191002.txt")
      
      println("18. Store the RDDs created in step 9, 12, 17 into HDFS locations")
      rejectdata.coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step18/rejectdata.txt")
      insuredatamerged.coalesce(1).saveAsTextFile("hdfs://localhost:54310/user/hduser/sparkhack2/output/step18/insuredatamerged.txt")
      
      
      
      
      /* VALIDATION
       * insuredatarepart.map(i=> i.BusinessDate.distinct).take(20).foreach { println }
      
      insuredatarepart.filter { x => x.BusinessDate.trim() == "01-10-2019" }.coalesce(1).
      saveAsTextFile("/home/hduser/01_10_2019")
      
      insuredatarepart.filter { x => x.BusinessDate.trim() == "02-10-2019" }.coalesce(1).
      saveAsTextFile("/home/hduser/02_10_2019")

      println(insuredatarepart.filter { x => x.BusinessDate.trim() == "01-10-2019" }.count())
      println(insuredatarepart.filter { x => x.BusinessDate.trim() == "02-10-2019" }.count())
      
      */
      
      println("19. Convert the RDD created in step 16 above into Dataframe namely insuredaterepartdf")
      val insureschema= StructType(Array(
          StructField("IssuerId",IntegerType,true),
          StructField("IssuerId2",IntegerType,true),
          StructField("BusinessDate", DateType,true),
          StructField("StateCode", StringType,true),
          StructField("SourceName", StringType,true),
          StructField("NetworkName", StringType,true),
          StructField("NetworkURL", StringType,true),
          StructField("custnum", IntegerType,true),
          StructField("MarketCoverage", StringType,true),
          StructField("DentalOnlyPlan", StringType,true)
              ))
       val insuredaterepartdf1 = sqlc.createDataFrame(sqlc.createDataFrame(insuredatarepart).rdd,schema=insureschema)
      
       /* VALIDATION
       *  insuredaterepartdf1.printSchema()
       */
       
        println("20. Create structuretype for all the columns as per the insuranceinfo1.csv")
        
        println("21. Create dataset using the csv module with option to escape ‘,’ accessing the insuranceinfo1.csv and insuranceinfo2.csv files, apply the schema of the structure type created in the step 20")
         
        
       val ds1=sqlc.read.format("csv")
                .schema(insureschema)
                .option("dateFormat", "yyyy-MM-dd")
                .option("header", "true")
                .option("nullValue","")
                .option("nanValue","0")
                .option("escape",",")
                .option("mode","dropmalformed")
                .csv("/home/hduser/workspacespark/New_workspace/Spark_core_usecsses/src/org/inceptez/sparkusecases/insuranceinfo1.csv")
                
                /*
                 * .as[Insureclass];
                 */
                 
      
       val ds2=sqlc.read.format("csv")
                .schema(insureschema)
                .option("dateFormat", "yyyy-MM-dd")
                .option("header", "true")
                .option("nullValue","")
                .option("nanValue","0")
                .option("escape",",")
                .option("mode","dropmalformed")
                .csv("/home/hduser/workspacespark/New_workspace/Spark_core_usecsses/src/org/inceptez/sparkusecases/insuranceinfo2.csv")
                
                /*
                 * .as[Insureclass];
                 */
     
      
      ds1.show(10,false)
      
      println("22. Apply the below DSL functions in the DFs created in step 21.")
      println("a. Rename the fields StateCode and SourceName as stcd and srcnm respectively.")
      println("b. Concat IssuerId,IssuerId2 as issueridcomposite and make it as a new field Hint : Cast to string and concat.")
      println("c. Remove DentalOnlyPlan column")
      println("d. Add columns that should show the current system date and timestamp with the fields name of sysdt and systs respectively.")
      
     ds1.withColumnRenamed("StateCode", "stcd")
        .withColumnRenamed("SourceName", "srcnm")
        .withColumn("issueridcomposite", concat(col("IssuerId"),col("IssuerId2")))
        .drop("DentalOnlyPlan")
        .withColumn("sysdt",current_date())
        .withColumn("systs",current_timestamp()).show()
    
     
        
    println("23. Remove the rows contains null in any one of the field and count the number of rows which contains all columns with some value. Hint: Use drop.na options")
      
    val ds3=ds1.na.drop()
    println(s"number of rows which contains all columns with some value =>  ${ds3.count()}")
    ds3.show(false)
    ds3.write.mode("overwrite").csv("hdfs://localhost:54310/user/hduser/sparkhack2/output/step23/ds1.csv")
    
    
    println("24. Custom Method creation: Create a package (org.inceptez.hack), class (allmethods),method (remspecialchar)")
    
    println("25. Import the package, instantiate the class and register the method generated in step 24 as a udf for invoking in the DSL function.")
    val reusableobj=new allmethods();
    val remspecialchar = udf(reusableobj.remspecialchar _)
    sqlc.udf.register("remspecialchar",reusableobj.remspecialchar _)
    
    
    println("26. Call the above udf in the DSL by passing NetworkName column as an argument to get the special characters removed DF")
                
     ds3.select(col("IssuerId"),
                col("IssuerId2"),
	              col("BusinessDate"),
                col("StateCode"),
                col("SourceName"),
                remspecialchar(col("NetworkName")),
                col("NetworkURL"),
                col("custnum"),
                col("MarketCoverage"),col("DentalOnlyPlan")).withColumnRenamed("UDF(NetworkName)", "NetworkName").show(false)
                
                
                
    
    /* VALIDATION */
    /*
     println("Validation start")
     println(reusableobj.remspecialchar("Pathway-/2X_(with dental)"))
     println(reusableobj.remspecialchar("Humana Dental PPO/Traditional Peferred"))
     println(reusableobj.remspecialchar("QHP PPO_OAMC_Chicago Illinois"))
     println("Validation end")
    */
    
               
    println("27. Save the DF generated in step 26 in JSON into HDFS with overwrite option.")
    ds3.select(col("IssuerId"),
                col("IssuerId2"),
	              col("BusinessDate"),
                col("StateCode"),
                col("SourceName"),
                remspecialchar(col("NetworkName")),
                col("NetworkURL"),
                col("custnum"),
                col("MarketCoverage")).withColumnRenamed("UDF(NetworkName)", "NetworkName").write.mode("overwrite").json("hdfs://localhost:54310/user/hduser/sparkhack2/output/step27/ds3.json")
      
    
    println("28. Save the DF generated in step 26 into CSV format with header name as per the DF and delimited by ~ into HDFS with overwrite option.")
    ds3.select(col("IssuerId"),
                col("IssuerId2"),
	              col("BusinessDate"),
                col("StateCode"),
                col("SourceName"),
                remspecialchar(col("NetworkName")),
                col("NetworkURL"),
                col("custnum"),
                col("MarketCoverage")).withColumnRenamed("UDF(NetworkName)", "NetworkName").write.mode("overwrite").option("header", "true").option("sep","~").csv("hdfs://localhost:54310/user/hduser/sparkhack2/output/step28/ds3_1.csv")
    
     ds3.createOrReplaceTempView("joinedview")
     
     sparkSession.sqlContext.sql("""select IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,
     NetworkURL,custnum,MarketCoverage,DentalOnlyPlan from joinedview""").show(false)    
     
     /* VALIDATION
     sparkSession.sqlContext.sql("""SELECT * FROM  default.custmaster""").show(5,false)
     sparkSession.sqlContext.sql("create database if not exists spark_hack")
     sparkSession.sqlContext.sql("""use spark_hack""")
     sparkSession.sqlContext.sql("""DROP TABLE IF EXISTS spark_hack.insuredata""")
     sparkSession.sqlContext.sql(""" CREATE TABLE IF NOT EXISTS spark_hack.insuredata 
                          (IssuerId Int,IssuerId2 Int,BusinessDate date,StateCode String,SourceName String,	
                          NetworkName String,	NetworkURL String,custnum Int,MarketCoverage String,DentalOnlyPlan String) 
                          ROW FORMAT DELIMITED 
                          FIELDS TERMINATED BY ',' 
                          LINES TERMINATED BY '\n'""")
     val temp=sparkSession.sqlContext.sql("""insert overwrite table spark_hack.insuredata select IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,
                         custnum,MarketCoverage,DentalOnlyPlan from joinedview """)
     temp.write.mode("overwrite").saveAsTable("spark_hack.insuredata")
     sparkSession.catalog.listDatabases.show(10,false);
   	 val results = sparkSession.sqlContext.sql("""select * from spark_hack.insuredata""")
		 results.show(false)
     *
     */
     
     
     
     println("29. Save the DF generated in step 26 into hive external table and append the data without overwriting it.")
     
     ds3.select(col("IssuerId"),
                col("IssuerId2"),
	              col("BusinessDate"),
                col("StateCode"),
                col("SourceName"),
                remspecialchar(col("NetworkName")),
                col("NetworkURL"),
                col("custnum"),
                col("MarketCoverage"),col("DentalOnlyPlan")).withColumnRenamed("UDF(NetworkName)", "NetworkName").
                write.mode("append").option("sep", ",").csv("hdfs://localhost:54310/user/hduser/sparkhack2/output/hive")
     
     /*  
     VALIDATION
         
     println("Below data will be produced as result on querying the external table")    
     
     ds3.select(col("IssuerId"),
                col("IssuerId2"),
	              col("BusinessDate"),
                col("StateCode"),
                col("SourceName"),
                remspecialchar(col("NetworkName")),
                col("NetworkURL"),
                col("custnum"),
                col("MarketCoverage"),col("DentalOnlyPlan")).withColumnRenamed("UDF(NetworkName)", "NetworkName").show(20,false)
         
    sparkSession.sqlContext.sql("""create external table if not exists insuredata_textfile
		(
		IssuerId Int,	
		IssuerId2 Int,
		BusinessDate String,
		StateCode String,	
		SourceName String,	
		NetworkName String,	
		NetworkURL String,	
		custnum Int,
		MarketCoverage String,
		DentalOnlyPlan String
		)
		row format delimited fields terminated by ','
		stored as textfile
		location 'hdfs://localhost:54310/user/hduser/sparkhack2/output/hive'""");*/
  
     
     sparkSession.sqlContext.sql("""from spark_hack.insuredata_textfile 
     select IssuerId,IssuerId2,BusinessDate,StateCode,SourceName,NetworkName,NetworkURL,custnum,MarketCoverage,DentalOnlyPlan""").show(20,false)
  
     
     
        
     println("30. Load the file3 (custs_states.csv) from the HDFS location, using textfile API")
     val custstates= sc.textFile("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
     
     println("31. Split the above data into 2 RDDs, first RDD namely custfilter should be loaded only with 5 columns data and second RDD namely statesfilter should be only loaded with 2 columns data. ")
     val custfilter=custstates.filter(x=> x.split(",").length==5)
     val statesfilter=custstates.filter(x=> x.split(",").length==2)
     
     /* VALIDATION
      * custfilter.take(10).foreach(println)
        statesfilter.take(10).foreach(println)
     */
     
     println("32. Load the file3 (custs_states.csv) from the HDFS location, using CSV Module")
     val cus_state=sqlc.read.format("csv").csv("hdfs://localhost:54310/user/hduser/sparkhack2/custs_states.csv")
     
     
     println("33. Split the above data into 2 DFs, first DF namely custfilterdf should be loaded only with 5 columns data and second DF namely statesfilterdf should be only loaded with 2 columns data.")
     val cust_df=cus_state.filter(col("_c2").isNotNull).withColumnRenamed("_c0","Id")
     .withColumnRenamed("_c1", "FirstName")
     .withColumnRenamed("_c2", "LastName")
     .withColumnRenamed("_c3", "Age")
     .withColumnRenamed("_c4", "Profession")
     
     val state_df=cus_state.filter(col("_c2").isNull).drop("_c2","_c3","_c4")
     .withColumnRenamed("_c0", "State_cd")
     .withColumnRenamed("_c1", "State_desc")
     
     
     println("34. Register the above DFs as temporary views as custview and statesview.")
     cust_df.createOrReplaceTempView("custview")
     state_df.createOrReplaceTempView("statesview")
     
        /* VALIDATION
         * cust_df.show(5,false)
       state_df.show(5,false)
     * */
     
     println("35. Register the DF generated in step 22 as a tempview namely insureview")
     val ds22=ds1.withColumnRenamed("StateCode", "stcd")
        .withColumnRenamed("SourceName", "srcnm")
        .withColumn("issueridcomposite", concat(col("IssuerId"),col("IssuerId2")))
        .drop("DentalOnlyPlan")
        .withColumn("sysdt",current_date())
        .withColumn("systs",current_timestamp())
     ds22.createOrReplaceTempView("insureview") 
     

     println("36. Import the package, instantiate the class and Register the method created in step 24 in the name of remspecialcharudf using spark udf registration.")
     val reusableobjudf=new allmethods();
     val remspecialcharudf = udf(reusableobjudf.remspecialchar _)
     sqlc.udf.register("remspecialcharudf",reusableobjudf.remspecialchar _)
  
     
      println("37. Write an SQL query with the below processing")
      println("a. Pass NetworkName to remspecialcharudf and get the new column called cleannetworkname")
      println("b. Add current date, current timestamp fields as curdt and curts.")
      println("c. Extract the year and month from the businessdate field and get it as 2 new fieldscalled yr,mth respectively.")
      println("d. Extract from the protocol either http/https from the NetworkURL column, if no protocol found then display noprotocol. For Eg: if http://www2.dentemax.com/ then show http if www.bridgespanhealth.com then show as noprotocol store in a column called protocol.")
      println("e. Display all the columns from insureview including the columns derived from above a, b, c, d steps with statedesc column from statesview with age,profession column from custview . Do an Inner Join of insureview with statesview using stcd=stated and join insureview with custview using custnum=custid.")

      /* VALIDATION
      sparkSession.catalog.listFunctions.filter('name like "%remspecialcharudf%").show(false)
      */
      
      sqlc.sql("""select 
            IssuerId,IssuerId2,BusinessDate,State_desc,srcnm,NetworkName,NetworkURL,custnum,MarketCoverage,issueridcomposite,sysdt,systs,
            remspecialcharudf(NetworkName) as cleannetworkname,
            current_date() as curdt,current_timestamp() as curts,
            year(BusinessDate) as yr,month(BusinessDate) as mth,
            case when instr(NetworkURL,"http")>0 then "http" else "noprotocol" end as Protocol
      
          from insureview i inner join statesview s on i.stcd==s.state_cd inner join custview c on i.custnum==c.Id""").show(5,false)
          
          
               
      sqlc.sql("""select 
            IssuerId,IssuerId2,BusinessDate,State_desc,srcnm,NetworkName,NetworkURL,custnum,MarketCoverage,issueridcomposite,sysdt,systs,
            remspecialcharudf(NetworkName) as cleannetworkname,
            current_date() as curdt,current_timestamp() as curts,
            year(BusinessDate) as yr,month(BusinessDate) as mth,
            case when instr(NetworkURL,"http")>0 then "http" else "noprotocol" end as Protocol,
            Profession,Age
      
          from insureview i inner join statesview s on i.stcd==s.state_cd inner join custview c on i.custnum==c.Id""").createOrReplaceTempView("comb_view")
          
       
       println("38. Store the above selected Dataframe in Parquet formats in a HDFS location as a single file.")     
            
       sqlc.sql("""select 
            IssuerId,IssuerId2,BusinessDate,State_desc,srcnm,NetworkName,NetworkURL,custnum,MarketCoverage,issueridcomposite,sysdt,systs,
            remspecialcharudf(NetworkName) as cleannetworkname,
            current_date() as curdt,current_timestamp() as curts,
            year(BusinessDate) as yr,month(BusinessDate) as mth,
            case when instr(NetworkURL,"http")>0 then "http" else "noprotocol" end as Protocol
      
          from insureview i inner join statesview s on i.stcd==s.state_cd inner join custview c on i.custnum==c.Id""").coalesce(1)
          .write.parquet("hdfs://localhost:54310/user/hduser/sparkhack2/output/Step38")
          
          
         
          
          println("39. Very very important interview question")

           
          sqlc.sql("""select row_number() over(PARTITION BY Protocol ORDER BY total desc) as seq_number,avg_age,total,State_desc,Protocol,Profession from 
                  (select avg(age) avg_age,count(1) as total,State_desc,Protocol,Profession from comb_view GROUP BY State_desc,Protocol,Profession)
                  """).show(50,false)
            
          val df39=sqlc.sql("""select row_number() over(PARTITION BY Protocol ORDER BY total desc) as seq_number,
                              avg_age,total,State_desc,Protocol,Profession 
                               from 
                              (select avg(age) avg_age,count(1) as total,State_desc,Protocol,Profession from comb_view 
                              GROUP BY State_desc,Protocol,Profession)""")
            
            /*
             * row_number() over(PARTITION BY Protocol ORDER BY count) as seq_number,
            sqlc.sql("""select avg(age),count(1) as count,statedec,protocol,profession
                       row_number() over(partition by protocol order by count desc ) as seq_number
                       from table_name groupby statedec,protocol,profession """)
            */
      
     
           println("40. Store the DF generated in step 39 into MYSQL table insureaggregated.")
           val url="jdbc:mysql://localhost:3306/custdb"
           val table ="insureaggregated"
           val connectionProperties=new Properties()
           connectionProperties.setProperty("user","root")
           connectionProperties.setProperty("password","root")
           connectionProperties.setProperty("driver","com.mysql.jdbc.Driver")
           df39.write.mode("overwrite").jdbc(url, table, connectionProperties)

 
  
  
  }
  
}