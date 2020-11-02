package com.google.ads.googleads.examples.remarketing;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkConf;



public class TestData {
   private static final Logger logger = LogManager.getLogger(TestData.class);
   
   public static void main(String[] args) 
   	  throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
   	  // Dataset<Row> ds = createCustomerContactDataset();
   	  // ds.show();
      	  
   	  //JavaRDD<String> CustomerContactRDD  = createCustomerContactRDD();
   }
   
   
   
   static JavaRDD<String> createCustomerContactRDD() {		  
      SparkConf conf = new SparkConf().setAppName("CustomerContactRDD").setMaster("local");
      JavaSparkContext sc = new JavaSparkContext(conf);
      sc.setLogLevel("TRACE");
   	  
      JavaRDD<String> customerContactRDD = sc.textFile("src/main/resources/customer_contact_10000.csv");
      return customerContactRDD ;
   }
   
   
   
   static Dataset<Row> createEmailDataset() {	  
      SparkSession spark = SparkSession.builder().appName("documentation").master("local").getOrCreate();
      spark.sparkContext().setLogLevel("ERROR");
      
      List<org.apache.spark.sql.types.StructField> listOfStructField=new ArrayList<org.apache.spark.sql.types.StructField>();
      listOfStructField.add(DataTypes.createStructField("accountId", DataTypes.StringType, true));
      listOfStructField.add(DataTypes.createStructField("email",     DataTypes.StringType, true));
      StructType structType=DataTypes.createStructType(listOfStructField);
      Dataset<Row> dataset =spark.createDataFrame(emailRowList(),structType);
      return dataset;
   }



   public static Dataset<Row> createCustomerContactDataset() {	   
      SparkSession spark = SparkSession.builder().appName("CustomerContact").master("local").getOrCreate();
      spark.sparkContext().setLogLevel("TRACE");
      
      Dataset<Row> dataset;
      
      dataset = spark.read()
      	             .format("csv")
      	             .option("header", "true")
      	             .option("delimiter", ",")
      	             .option("escape", "\"")
                     .load("src/main/resources/customer_contact_10000.csv");
      
      dataset.printSchema();
      logger.info("customerContactDataSet count=" + dataset.count());
      
      return dataset;
   }



   private static List<Row> emailRowList() {
      List<Row> rows = new ArrayList<>();
      
      rows.add(RowFactory.create("1","dinesh1@gmail.com"));
      rows.add(RowFactory.create("2","dinesh2@gmail.com"));
      rows.add(RowFactory.create("3","dinesh3@gmail.com"));
      rows.add(RowFactory.create("4","dinesh4@gmail.com"));
      rows.add(RowFactory.create("5","dinesh5@gmail.com"));
      rows.add(RowFactory.create("6","dinesh6@gmail.com"));
      rows.add(RowFactory.create("7","dinesh7@gmail.com"));
      rows.add(RowFactory.create("8","dinesh8@gmail.com"));
      rows.add(RowFactory.create("9","dinesh9@gmail.com"));
      return rows;
   }  
}
