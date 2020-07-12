package org.inceptez.sparkusecases

import org.apache.spark.sql.{SparkSession,DataFrame}
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.{col,expr,split,explode}
import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType ,DateType}
import org.apache.spark.SparkContext
import scala.concurrent.duration.Duration
import org.apache.spark.sql.streaming.{Trigger}
 

object sparkstreaming 
{
  
  val spark=SparkSession.builder().appName("STREAMING")
                                  .master("local[2]")
                                /*.config("spark.sql.streaming.checkpointLocation","file:///tmp")*/
                                  .getOrCreate()
  
  
  
  import spark.implicits._
    
    def readFromSocket()=
      
    {
  
    println("1. Load the chatdata into a Dataframe using ~ delimiter by pasting in kafka or streaming file source or socket with the seconds of 50.")
    
    /*SOCKET STREAMING*/
      /*val lines:DataFrame=spark.readStream
                               .format("socket")
                               .option("host","localhost")
                               .option("port",12345)
                               .load()*/
    
    /*FILE STREAMING*/
      val schema=StructType(Array(StructField("value",StringType)))
      val lines=spark.readStream
                      .schema(schema)
                      .csv("/home/hduser/workspacespark/New_workspace/Spark_core_usecsses/src/org/inceptez/sparkusecases/chatdata")
                      .toDF("value")
    
                      
     
     println("2. Define the column names as id,chat,'type' where id is the customer who is chatting with the support agent, chat is the plain text"+
             "of the chat message, type is the indicator of c means customer or a means agent. ")
     val data=lines.select(expr("cast(value as string) as data"))
           .withColumn("id", split(col("data"),"~").getItem(0))
           .withColumn("chat", split(col("data"),"~").getItem(1))
           .withColumn("type", split(col("data"),"~").getItem(2)).drop("data")     
           
         
       
     println("3. Filter only the records contains 'type' as 'c' (to filter only customer interactions)")     
     val date_prep=data.select("id","chat").where(col("type")==="c")
    
     
     println("4. Remove the column 'type' from the above dataframe, hence the resultant dataframe contains only id and chat and convert to tempview.")
     date_prep.createOrReplaceTempView("date_prep")
    
     println("5. Use SQL split function on the 'chat' column with the delimiter as ' ' (space) to tokenize all words")
     
     println("6. Use SQL explode function to pivot the above data created in step 5, then the exploded/pivoted data looks like below,"+
             "register this as a tempview.")

     spark.sql("select id,SPLIT(chat,' ') as chat_array from date_prep")
          .select('id,explode('chat_array).as("word"))
          .createOrReplaceTempView("cust_view")
    
     println("8. Load the stopwords from linux file location /home/hduser/stopwordsdir/stopwords (A single column file found in the web that"+
            " contains values such as (a,above,accross,after,an,as,at) etc. which is used in any English sentences for punctuations or as fillers)"+
            " into dataframe with the column name stopword and convert to tempview.")
     
      val stopwords=spark.read.csv("/home/hduser/workspacespark/New_workspace/Spark_core_usecsses/src/org/inceptez/sparkusecases/stopwords")
                             .withColumnRenamed("_c0", "stopword")
    
      stopwords.createOrReplaceTempView("stopwordsview")
      
      
      
     println("9. Write a left outer join between chat tempview created in step 6 and stopwords tempview created in step 8 and filter all nulls"+
     "(or) use subquery with not in option to filter all stop words from the actual chat tempview using the stopwords tempview created above.")
     
     println("temp DF creation start")
      
     val temp=spark.sql(""" SELECT id,word from cust_view where word not in (select stopword from stopwordsview) """)
      
     
      temp.createOrReplaceTempView("temp_view")
      
      println("temp DF creation end")
      
      
      
      println("11. Identify the most recurring keywords used by the customer in all the chats by grouping based on the"+
              "keywords used with count of keywords. use group by and count functions in the sql")
      
      println("occurances DF creation start")
     
      val occurances=spark.sql(""" SELECT word chatkeywords,count(1) occurances from temp_view group by word """)
      spark.conf.set("spark.sql.shuffle.partitions","1")
      occurances.createOrReplaceTempView("occurances_view")
      println("occurances DF creation end")
      println("query0 execution start")
      val query0=temp.writeStream
                     .format("memory")
                     .outputMode("append")
                     /*
                      * .option("checkpointLocation","file:///home/hduser/tmp_1")
                      */
                     .queryName("res1")
                     .start()
                    
                     
                     
       
       Thread.sleep(10000) 
       println("10. Load the final result into a hive table should have only result as given below using append option.")

        
       println("Sleep completed")
       spark.sql(""" SELECT id,word from res1""").show(10,false)
       println("Started writing")
       spark.sql(""" SELECT id,word from res1""").coalesce(1).write.mode("append")
       .parquet("hdfs://localhost:54310/user/hive/warehouse/spark_hack.db/stream")
       println("Completed writing")
       query0.awaitTermination(50000)
       println("query0 execution end")
                
       
       
       
       
       println("12. Store the above result in a json format with column names as chatkeywords, occurance")
       
       println("query execution start")
       val query=occurances.writeStream
                     .format("memory")
                     .outputMode("complete")
                     /*
                      * .option("checkpointLocation","file:///home/hduser/tmp_2")
                      */
                     .queryName("res2")
                     .start()
       
       Thread.sleep(5000) 
       println("Sleep completed")
       spark.sql(""" SELECT chatkeywords,occurances from res2""").show(10,false)
       println("Started writing")
       spark.sql(""" SELECT chatkeywords,occurances from res2""")
            .coalesce(1).write.mode("append")
            .json("hdfs://localhost:54310/user/hduser/sparkhack2/output//res.json") 
       
            println("Completed writing")
       query.awaitTermination(10000) 
       println("query execution end")
       
             
      
    }
  
 
        def main(args: Array[String]) 
        {
          readFromSocket()
 
         }
}