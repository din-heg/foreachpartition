package com.google.ads.googleads.examples.remarketing
 


import scala.collection.JavaConverters._
import org.apache.logging.log4j.{ LogManager, Logger }
import org.apache.logging.log4j.LogManager
import org.apache.logging.log4j.Logger
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkConf
import com.google.ads.googleads.v5.services.OfflineUserDataJobOperation
import scala.Iterator



object MasterMindOfflineJob extends Serializable{ 
    val logger: Logger                                  = LogManager.getLogger(getClass)
    def main(args: Array[String]) = {     
       lazy val sparkConf = new SparkConf().setAppName("CustomerContact")
                                           .setMaster("local[*]")
                                           .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                                           //.set("spark.kryo.registrationRequired", "true")
                                           .registerKryoClasses(Array(Class.forName("com.google.ads.googleads.v5.services.OfflineUserDataJobServiceClient")))
       lazy val spark = SparkSession.builder()
                                    .config(sparkConf)
                                    .getOrCreate()                                                 
     
	     spark.sparkContext.setLogLevel("WARN");

       logger.info("main started")
       val test = new TestOfflineJob()
       test.runStandalone(spark)

       logger.info("main Ended")
    }
  
}

class TestOfflineJob extends Serializable{
   val logger: Logger                                  = LogManager.getLogger(getClass)
   logger.info("Running");
   
   
   def runStandalone(spark: SparkSession) {
     System.setProperty("standAlone", "true")
     // Test.runAsList(spark)
     // TestGoogleExport.runForEachOnline(spark)
     TestOfflineJob.runForEachOffline(spark)
     // Test.runMap(spark)
     //TestGoogleExport.runForEachNoSerialized(spark)       
   }
} 




object TestOfflineJob extends Serializable{ 
   val logger: Logger                       = LogManager.getLogger(getClass) 
                                            
   lazy val offlineUserDataJobOperations    = new java.util.ArrayList[OfflineUserDataJobOperation]();
  
    def createCustomerContactDataset(spark: SparkSession):Dataset[Row] = {	   	     
	     val dataset = spark.read
		   	         .format("csv")
		   	         .option("header", "true")
		   	         .option("delimiter", ",")
		   	         .option("escape", "\"")
	               .load("src/main/resources/customer_contact_10000.csv");
	     
	     dataset.printSchema();
	     logger.info("customerContactDataSet count=" + dataset.count());
       dataset; 
	 }
    
   def runForEachOffline(spark: SparkSession) {
      logger.info("--------------------------------------------------------------------------------------------------------")
      logger.info("------------------ -----F O R    E A C H   O F F L I N E -----------------------------------------------")
      logger.info("--------------------------------------------------------------------------------------------------------")

      val customerMatchUploadDf = createCustomerContactDataset(spark).repartition(5)
      logger.info(s"count=${customerMatchUploadDf.count}")

      val  addCustomerMatchUserListOffline = new AddCustomerMatchUserListOffline()
      lazy val googleAdsClient                 = GoogleAdsUtil.getGoogleAdsClient() 
      val loginCustomerId:Long                 = googleAdsClient.getLoginCustomerId()
      lazy val userListResourceName            = addCustomerMatchUserListOffline.createCustomerMatchUserList(googleAdsClient, loginCustomerId)

      customerMatchUploadDf.foreachPartition(itr => 
	        addCustomerMatchUserListOffline.buildOfflineUserDataJobOperations(loginCustomerId, userListResourceName, itr.asJava))
 
      
      // customerMatchUploadDf.foreachPartition(itr => offlineUserDataJobServiceClient.addOfflineUserDataJobOperations(
	    //     AddOfflineUserDataJobOperationsRequest.newBuilder()
	    //        .setResourceName(this.offlineUserDataJobResourceName)
	    //        .setEnablePartialFailure(BoolValue.of(true))
	    //         .addAllOperations(offlineUserDataJobOperations)
 	    //         .build()))

      logger.info("Completed foreachPartition")           
      //OfflineJob.runOffline(offlineUserDataJobServiceClient, offlineUserDataJobResourceName)    
      //OfflineJob.checkJobStatus(googleAdsClient, loginCustomerId, offlineUserDataJobResourceName)
   }

    
   /*
   def runAsList(spark: SparkSession) {
      logger.info("")
      logger.info("--------------------------------------------------------------------------------------------------------")
      logger.info("----------------------------------A  S    L  I  S  T----------------------------------------------------")
      logger.info("--------------------------------------------------------------------------------------------------------")
      logger.info("")
      val customerMatchUploadDf = createCustomerContactDataset(spark)
      logger.info(s"count=${customerMatchUploadDf.count}")
      
      OfflineJob.buildOfflineUserDataJobOperationsList(offlineUserDataJobServiceClient, loginCustomerId, userListResourceName, customerMatchUploadDf)
      logger.info("Completed foreachPartition")           
      OfflineJob.runOffline(offlineUserDataJobServiceClient, offlineUserDataJobResourceName)    
      OfflineJob.checkJobStatus(googleAdsClient, loginCustomerId, offlineUserDataJobResourceName)
   }

   def runForEachOnline(spark: SparkSession) {
      logger.info("--------------------------------------------------------------------------------------------------------")
      logger.info("---------------------------------- F O R    E A C H  O N L I N E ---------------------------------------")
      logger.info("--------------------------------------------------------------------------------------------------------")

      val customerMatchUploadDf = createCustomerContactDataset(spark).repartition(5)
      logger.info(s"count=${customerMatchUploadDf.count}")
      
      import com.sony.sie.kamaji.mlmodel.google.AddCustomerMatchUserListOnline
      import spark.implicits._
      customerMatchUploadDf.foreachPartition(itr =>  
         new AddCustomerMatchUserListOnline().buildUserListForEach(googleAdsClient, loginCustomerId, userListResourceName, itr.asJava)) 
   }




   def runMap(spark: SparkSession) {
      logger.info("--------------------------------------------------------------------------------------------------------")
      logger.info("---------------------------------- M  A  P ------------------------------------------------------------")
      logger.info("--------------------------------------------------------------------------------------------------------")

     val customerMatchUploadDf = createCustomerContactDataset(spark)
      logger.info(s"count=${customerMatchUploadDf.count}")
      
      import spark.implicits._
      case class Record(accountId:String, firstName:String, lastName:String, emailAddress:String, zipCode:String, countryCode:String)
      //customerMatchUploadDf.as[Record]
      val np = customerMatchUploadDf.mapPartitions(itr => OfflineJob.buildOfflineUserDataJobOperationsMap(offlineUserDataJobServiceClient, loginCustomerId, userListResourceName, itr.asJava).asScala)
      np.show()
      logger.info("Sum=" + np.reduce(_ + _))
      logger.info("Completed foreachPartition")           
      OfflineJob.runOffline(offlineUserDataJobServiceClient, offlineUserDataJobResourceName)    
      OfflineJob.checkJobStatus(googleAdsClient, loginCustomerId, offlineUserDataJobResourceName)
   }

   
   def runForEachNoSerialized(spark: SparkSession) {
      logger.info("--------------------------------------------------------------------------------------------------------")
      logger.info("----------------- T  A  S  K    N  O  T     S  E  R  I  A  L  I  Z  E  D -------------------------------")
      logger.info("--------------------------------------------------------------------------------------------------------")
     
      val customerMatchUploadDf = createCustomerContactDataset(spark)
      logger.info(s"count=${customerMatchUploadDf.count}")

      val googleAdsClient                 = GoogleAdsUtil.getGoogleAdsClient() 
      val userListResourceName            = OfflineJob.createCustomerMatchUserList(googleAdsClient, loginCustomerId)
      val pair                            = OfflineJob.creatOfflineJob2(googleAdsClient, loginCustomerId, userListResourceName)
      val offlineUserDataJobServiceClient = pair.getValue0()
      val offlineUserDataJobResourceName  = pair.getValue1()
 
      uploadToGoogleAds(customerMatchUploadDf, 
                         offlineUserDataJobServiceClient, 
                         loginCustomerId, 
                         userListResourceName, 
                         offlineUserDataJobResourceName) 
   }

   def uploadToGoogleAds(df                              :DataFrame, 
                         offlineUserDataJobServiceClient :OfflineUserDataJobServiceClient, 
                         loginCustomerId                 :Long, 
                         userListResourceName            :String, 
                         offlineUserDataJobResourceName  :String) {
      logger.info("Google Ads - Building Offline User Data Job Operations");     
      
      df.foreachPartition(itr => OfflineJob.buildOfflineUserDataJobOperations(loginCustomerId, userListResourceName, itr.asJava))
      logger.info("Google Ads - Submitting Jobs to run offline")
      OfflineJob.runOffline(offlineUserDataJobServiceClient, offlineUserDataJobResourceName)
   }
   

    
   */   

   
   
  
   
   def sumfuncpartition(numbers : Iterator[Row]) : Iterator[Int] = {
     var sum = 1
     while (numbers.hasNext) {
         sum = sum + 1
     }
     return Iterator(sum)
   }
   
}