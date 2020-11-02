# foreachpartition


## Configurations
Before building the jar file you need to 
### Properties file
The Google Ads propeties contains the developer token, credentials etc. You need to move your ads properties to src/main/resources. Set the  `propertiesFileName` variable in src/main/java/com/google/ads/googleads/examples/remarketing/GoogleAdsUtil.java  to "src/main/resources/\<your-property-filename\>"


### hadoop home
Change the Hadoop home otherwise you will get the "Could not locate executable null\bin\winutils.exe in the Hadoop binaries." error. 
In the main() method of src/main/java/com/google/ads/googleads/examples/remarketing/AddCustomerMatchUserListOptimized.java modify this statement, set it to your hadoop home:
`System.setProperty("hadoop.home.dir", "XXXXXXXXXXXXXXXXXX");`

If you do not have a hadoop home the program might run fine despite the "... winutils.exe .." error.




## System Requirements to build jar
- JDK 1.8
- Maven 3.3+


## Regular Mode and Spark Mode
The main() method of src/main/java/com/google/ads/googleads/examples/remarketing/AddCustomerMatchUserListOptimized.java
contains 2 sections: 
1) Run as a regular java program 
2) run inside the foreachPartition() method of spark dataframe

When you invoke the buildOfflineUserDataJobOperationsList() method it invokes the Google Ads API in regular mode(ie it does not run within spark)
This code is currently commented out. 

The other section of the main() method invokes the Google Ads APis within spark. It currently hangs when when invoking the offlineUserDataJobServiceClient.addOfflineUserDataJobOperations() method


