// Copyright 2020 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.ads.googleads.examples.remarketing;


import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import com.google.ads.googleads.examples.utils.ArgumentNames;
import com.google.ads.googleads.examples.utils.CodeSampleParams;
import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.ads.googleads.v5.common.CrmBasedUserListInfo;
import com.google.ads.googleads.v5.common.CustomerMatchUserListMetadata;
import com.google.ads.googleads.v5.common.OfflineUserAddressInfo;
import com.google.ads.googleads.v5.common.UserData;
import com.google.ads.googleads.v5.common.UserData.Builder;
import com.google.ads.googleads.v5.common.UserIdentifier;
import com.google.ads.googleads.v5.enums.CustomerMatchUploadKeyTypeEnum.CustomerMatchUploadKeyType;
import com.google.ads.googleads.v5.enums.OfflineUserDataJobStatusEnum.OfflineUserDataJobStatus;
import com.google.ads.googleads.v5.enums.OfflineUserDataJobTypeEnum.OfflineUserDataJobType;
import com.google.ads.googleads.v5.errors.GoogleAdsError;
import com.google.ads.googleads.v5.errors.GoogleAdsException;
import com.google.ads.googleads.v5.resources.OfflineUserDataJob;
import com.google.ads.googleads.v5.resources.UserList;
import com.google.ads.googleads.v5.services.AddOfflineUserDataJobOperationsRequest;
import com.google.ads.googleads.v5.services.AddOfflineUserDataJobOperationsResponse;
import com.google.ads.googleads.v5.services.CreateOfflineUserDataJobResponse;
import com.google.ads.googleads.v5.services.GoogleAdsRow;
import com.google.ads.googleads.v5.services.GoogleAdsServiceClient;
import com.google.ads.googleads.v5.services.MutateUserListsResponse;
import com.google.ads.googleads.v5.services.OfflineUserDataJobOperation;
import com.google.ads.googleads.v5.services.OfflineUserDataJobServiceClient;
import com.google.ads.googleads.v5.services.SearchGoogleAdsStreamRequest;
import com.google.ads.googleads.v5.services.SearchGoogleAdsStreamResponse;
import com.google.ads.googleads.v5.services.UserListOperation;
import com.google.ads.googleads.v5.services.UserListServiceClient;

import com.google.api.gax.longrunning.OperationFuture;
import com.google.api.gax.rpc.ServerStream;
import com.google.protobuf.BoolValue;
import com.google.protobuf.Int64Value;
import com.google.protobuf.StringValue;
import com.google.protobuf.Empty;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.config.Configurator;
import org.apache.logging.log4j.Level;

import com.google.common.collect.ImmutableList;

import com.beust.jcommander.Parameter;

import org.joda.time.Instant;

import org.apache.spark.TaskContext;

import org.javatuples.Pair; 


public class AddCustomerMatchUserList  implements java.io.Serializable {
  private static final long   serialVersionUID            = 1L;	
  private static final Logger logger                      = LogManager.getLogger(AddCustomerMatchUserList.class);	
  private static final String STAND_ALONE_PROPERTY_NAME   = "standAlone";
  
  private static final int    USER_IDENTIFIER_DATASET_SIZE      = 100000000; // Hundred Million
  
  private static final int 	  USER_IDENTIFIER_LIMIT             = 11;
  private static final int    OFFLINE_USER_JOB_OPERATIONS_LIMIT = 50000;
  
  private static final GoogleAdsClient                 googleAdsClient; 
  private static final Long                            loginCustomerId;  
  private static final String                          userListResourceName;
  private static final OfflineUserDataJobServiceClient offlineUserDataJobServiceClient;
  private static final String                          offlineUserDataJobResourceName;
  
  static {
	 try {
	  googleAdsClient                 = GoogleAdsUtil.getGoogleAdsClient(); 
      loginCustomerId                 = googleAdsClient.getLoginCustomerId();  
	  userListResourceName            = AddCustomerMatchUserList.createCustomerMatchUserList(googleAdsClient, loginCustomerId);
	  Pair<OfflineUserDataJobServiceClient, String> pair = creatOfflineJob(googleAdsClient, loginCustomerId, userListResourceName);
	  offlineUserDataJobServiceClient = pair.getValue0();
	  offlineUserDataJobResourceName  = pair.getValue1();
  } catch(Exception e){
      System.out.println("Exception while initializing" + e.getMessage());
      throw new RuntimeException(e.getMessage());
   }
	  
  }
  
  private static boolean nonEmpty(String str) {
	  return (str != null && !str.trim().isEmpty());
  }



  public static void main(String[] args)
      throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {
	  Configurator.setRootLevel(Level.INFO);
	  
	  System.setProperty("hadoop.home.dir", "C:\\Users\\dhegde1\\programs\\hadoop");
	  System.setProperty(STAND_ALONE_PROPERTY_NAME, "true");
	  

	  // buildOfflineUserDataJobOperationsList(offlineUserDataJobServiceClient,loginCustomerId,offlineUserDataJobResourceName,  TestData.createCustomerContactDataset());
	  
     	  
     Dataset<Row> customerMatchUploadDf = TestData.createCustomerContactDataset();
	 logger.info("customerMatchUploadDf.count() =" + customerMatchUploadDf.count());
	 customerMatchUploadDf.foreachPartition(itr -> AddCustomerMatchUserList.buildOfflineUserDataJobOperations(offlineUserDataJobServiceClient, loginCustomerId, userListResourceName, itr));
	  
     logger.info("Completed foreachPartition");           
     AddCustomerMatchUserList.runOffline(offlineUserDataJobServiceClient, offlineUserDataJobResourceName);    
     AddCustomerMatchUserList.checkJobStatus(googleAdsClient, loginCustomerId, offlineUserDataJobResourceName);
  }



  private static class AddCustomerMatchUserListParams extends CodeSampleParams {
	    @Parameter(names = ArgumentNames.CUSTOMER_ID, required = true)
	    private Long customerId;
  }



   /**
    * Creates a Customer Match user list.
    *
    * @param googleAdsClient the Google Ads API client.
    * @param customerId the client customer ID.
    * @return the resource name of the newly created user list.
    */
   public static String createCustomerMatchUserList(GoogleAdsClient googleAdsClient, long customerId) {
      // Creates the new user list.
	   
	   
	  logger.info("Creates the new user list.");
      UserList userList =
          UserList.newBuilder()
              .setName(StringValue.of("Customer Offline Match list ForeachPartition #" + System.currentTimeMillis()))
              .setDescription(StringValue.of("An offline list of customers containing emails and location created at " + Instant.now()))
              // Customer Match user lists can use a membership life span of 10,000 to indicate unlimited; otherwise normal values apply.
              // Sets the membership life span to 30 days.
              .setMembershipLifeSpan(Int64Value.of(30))
              // Sets the upload key type to indicate the type of identifier that will be used to
              // add users to the list. This field is immutable and required for an ADD operation.            
              .setCrmBasedUserList(
                  CrmBasedUserListInfo.newBuilder()
                      .setUploadKeyType(CustomerMatchUploadKeyType.CONTACT_INFO))            
              .setEligibleForDisplay(BoolValue.of(true))
              .setEligibleForSearch(BoolValue.of(true))
              .build();
      
      // Creates the operation.
      logger.info("Creates UserListOperation.");
      UserListOperation operation = UserListOperation.newBuilder().setCreate(userList).build();
      
      // Creates the service client.
      logger.info("userListServiceClient()");
      try (UserListServiceClient userListServiceClient =        googleAdsClient.getLatestVersion().createUserListServiceClient()) {
         // Adds the user list.
         logger.info("userListServiceClient.mutateUserLists()");	
         MutateUserListsResponse response =          userListServiceClient.mutateUserLists(Long.toString(customerId), ImmutableList.of(operation));
         // Prints the response.
         logger.printf(Level.INFO, "Created Customer Match user list with resource name: %s.%n", response.getResults(0).getResourceName());
         return response.getResults(0).getResourceName();
      }
   }
   


   public static Pair<OfflineUserDataJobServiceClient, String> creatOfflineJob(GoogleAdsClient googleAdsClient,
                                                                               long            customerId, 
                                                                               String          userListResourceName)
      throws InterruptedException, ExecutionException, TimeoutException, UnsupportedEncodingException {      
      try (OfflineUserDataJobServiceClient offlineUserDataJobServiceClient = googleAdsClient.getLatestVersion().createOfflineUserDataJobServiceClient()) {     
         // Creates a new offline user data job.
         OfflineUserDataJob offlineUserDataJob =  OfflineUserDataJob.newBuilder()
                 .setType(OfflineUserDataJobType.CUSTOMER_MATCH_USER_LIST)
                 .setCustomerMatchUserListMetadata(CustomerMatchUserListMetadata.newBuilder()
                                                                                .setUserList(StringValue.of(userListResourceName)))
                 .build();
         
         // Issues a request to create the offline user data job.
         CreateOfflineUserDataJobResponse createOfflineUserDataJobResponse =	offlineUserDataJobServiceClient.createOfflineUserDataJob(Long.toString(customerId), offlineUserDataJob);
         String offlineUserDataJobResourceName = createOfflineUserDataJobResponse.getResourceName();
         logger.printf(Level.INFO,"Created an offline user data job with resource name: %s,  offlineUserDataJobServiceClient=%s.%n", offlineUserDataJobResourceName, offlineUserDataJobServiceClient);
         return new Pair<OfflineUserDataJobServiceClient, String> (offlineUserDataJobServiceClient, offlineUserDataJobResourceName);      
      }
   }

   

   public static void runOffline(OfflineUserDataJobServiceClient offlineUserDataJobServiceClient, String offlineUserDataJobResourceName) {
   //     printSummary();
      
      logger.info("Issues an asynchronous request to run the offline user data job for executing all added operations.");
      OperationFuture<Empty, Empty> runFuture = offlineUserDataJobServiceClient.runOfflineUserDataJobAsync(offlineUserDataJobResourceName); 
      logger.info("Asynchronous request to execute the added operations started.");
      logger.info("Waiting until operation completes.");    	    
      // checkJobStatus(googleAdsClient, getLoginCustomerId(), offlineUserDataJobResourceName);
   }
   


   public  static void buildOfflineUserDataJobOperations(OfflineUserDataJobServiceClient offlineUserDataJobServiceClient,
                                                         long                            customerId,
                                                         String                          offlineUserDataJobResourceName, 
                                                         java.util.Iterator<Row>         iterator)   
   throws UnsupportedEncodingException {
	  logger.info("--------------------------------------------------------------------------------------------------------");
	  logger.info("----------------------------------F O R    E A C H    P A R T I T I O N---------------------------------");
	  logger.info("--------------------------------------------------------------------------------------------------------");
 
      logger.printf(Level.INFO,  "offlineUserDataJobServiceClient.getOfflineUserDataJob(resourceName)=%s", offlineUserDataJobServiceClient.toString());
      logger.printf(Level.INFO,  "offlineUserDataJobServiceClient.isShutdown()=%b", offlineUserDataJobServiceClient.isShutdown());
            
      List<Builder> userDatas                                        = getUserDatas();    
      List<OfflineUserDataJobOperation> offlineUserDataJobOperations = new ArrayList<>();
      Integer userIdentifierCount               = 0;
      Integer userDataIndex                     = 0;
      Integer totIdCount                        = 0;
      Long    receivedOperationCount            = 0l;	  
      
      long    emailSkipped                      = 0l;
      long    addressSkipped                    = 0l;
      long    postalCodeSkipped                 = 0l;
      long    countryCodeSkipped                = 0l;
      
//      uploadUserListOffline(offlineUserDataJobServiceClient, customerId, offlineUserDataJobResourceName, offlineUserDataJobOperations, 0l);
      
      
      logger.info("Get instance of sha256Digest");	     
      int userIdentifierDataSetCount    = 0;   
      
      MessageDigest sha256Digest;   
      try {   
         sha256Digest = MessageDigest.getInstance("SHA-256");   
      } catch (NoSuchAlgorithmException e) {   
    	   throw new RuntimeException("Missing SHA-256 algorithm implementation", e);   
      }   
      int partitionId = TaskContext.getPartitionId();
      logger.printf(Level.INFO, "Running buildOfflineUserDataJobOperations for partition %d", partitionId);
      while(iterator.hasNext()) {
         //logger.printf(Level.INFO, "partitionId=%d, totCount=%d, userDatas.get(%d).UserIdentifiersCount=%d", partitionId, userIdentifierDataSetCount, userDataIndex, userDatas.get(userDataIndex).getUserIdentifiersCount());
         Row userIdentiferInputList = (Row) iterator.next();
         String accountId   = userIdentiferInputList.getString(0);
         String firstName   = userIdentiferInputList.getString(1);
         String lastName    = userIdentiferInputList.getString(2);
         String email       = userIdentiferInputList.getString(3);
         String postalCode  = userIdentiferInputList.getString(4);
         String countryCode = userIdentiferInputList.getString(5);
         
         if (nonEmpty(email)) {
            userDatas.get(userDataIndex).addUserIdentifiers(UserIdentifier.newBuilder() 
                                         .setHashedEmail(StringValue.of(normalizeAndHash(sha256Digest, email)))
                                         .build());
            userIdentifierCount++;
            addDataOperations(offlineUserDataJobServiceClient, customerId, offlineUserDataJobResourceName,
            offlineUserDataJobOperations,
            userDatas,
            userIdentifierCount,
            userIdentifierCount,
            userDataIndex,
            totIdCount,
            receivedOperationCount,                                           
         false);
         } else {
            emailSkipped++;	
            logger.info("Skipping account {} because email address is missing." , accountId);
         }
         if (nonEmpty(lastName) && nonEmpty(firstName)) {
            OfflineUserAddressInfo.Builder offlineUserAddressInfoBuilder = OfflineUserAddressInfo.newBuilder()
                                                                  .setHashedFirstName(StringValue.of(normalizeAndHash(sha256Digest, firstName)))
                                                                .setHashedLastName(StringValue.of(normalizeAndHash(sha256Digest, lastName)));
            
            if (nonEmpty(postalCode))  offlineUserAddressInfoBuilder.setCountryCode(StringValue.of(postalCode)); else postalCodeSkipped++;
            if (nonEmpty(countryCode)) offlineUserAddressInfoBuilder.setPostalCode(StringValue.of(countryCode));   else countryCodeSkipped++;
            
            userDatas.get(userDataIndex).addUserIdentifiers(UserIdentifier.newBuilder()
                                .setAddressInfo(offlineUserAddressInfoBuilder.build()));
            userIdentifierCount++;
            addDataOperations(offlineUserDataJobServiceClient, customerId, offlineUserDataJobResourceName,
            offlineUserDataJobOperations,
            userDatas,
            userIdentifierCount,
            userIdentifierCount,
            userDataIndex,
            totIdCount,
            receivedOperationCount,                                           
            false);
         } else {
         addressSkipped++;	
            logger.info("Skipping account {} because either first and last names or both missing.", accountId);
         }
         userIdentifierDataSetCount++;
      
      } // while(iterator.hasNext()) 
      addDataOperations(offlineUserDataJobServiceClient, 
    		            customerId, 
    		            offlineUserDataJobResourceName,
                        offlineUserDataJobOperations,
                        userDatas,
                        userIdentifierCount,
                        userIdentifierCount,
                        userDataIndex,
                        totIdCount,
                        receivedOperationCount,                                           
                        true);
      logger.printf(Level.INFO, "Partition:%d  => UserList contains a total of %d data operations and %d user Identifiers ",     partitionId, receivedOperationCount,     totIdCount);
      logger.printf(Level.INFO, "Partition:%d  => Received: %d, Skip Record Count: Email:%d, Address:%d, Postal:%d, Country=%d", partitionId, userIdentifierDataSetCount, emailSkipped, addressSkipped,    postalCodeSkipped, countryCodeSkipped);
   }   

   
   
   public  static void buildOfflineUserDataJobOperationsList(OfflineUserDataJobServiceClient offlineUserDataJobServiceClient,
		                                          long                            customerId,
		                                          String                          offlineUserDataJobResourceName,
		                                          Dataset<Row>                    UserIdentifierDataSet)
				   throws UnsupportedEncodingException {
	   
	      logger.info("--------------------------------------------------------------------------------------------------------");
	      logger.info("----------------------------------A  S    L  I  S  T----------------------------------------------------");
	      logger.info("--------------------------------------------------------------------------------------------------------");

	      
	      logger.printf(Level.INFO,  "offlineUserDataJobServiceClient.getOfflineUserDataJob(resourceName)=%s", offlineUserDataJobServiceClient.toString());
	      logger.printf(Level.INFO,  "offlineUserDataJobServiceClient.isShutdown()=%b", offlineUserDataJobServiceClient.isShutdown());
	            
	      List<Builder> userDatas                                        = getUserDatas();    
	      List<OfflineUserDataJobOperation> offlineUserDataJobOperations = new ArrayList<OfflineUserDataJobOperation>();
	      Integer userIdentifierCount               = 0;
	      Integer userDataIndex                     = 0;
	      Integer totIdCount                        = 0;
	      Long    receivedOperationCount            = 0l;	  
	      
	      long    emailSkipped                      = 0l;
	      long    addressSkipped                    = 0l;
	      long    postalCodeSkipped                 = 0l;
	      long    countryCodeSkipped                = 0l;
	      
	      logger.info("Get instance of sha256Digest");	     
	      int userIdentifierDataSetCount    = 0;   
	      
	      MessageDigest sha256Digest;   
	      try {   
	         sha256Digest = MessageDigest.getInstance("SHA-256");   
	      } catch (NoSuchAlgorithmException e) {   
	    	   throw new RuntimeException("Missing SHA-256 algorithm implementation", e);   
	      }   
	      int partitionId = TaskContext.getPartitionId();
	      logger.printf(Level.INFO, "Running buildOfflineUserDataJobOperations for partition %d, UserIdentifierDataSet.count()=%d", partitionId, UserIdentifierDataSet.count());
	      List<Row> userIdentiferInputList = UserIdentifierDataSet.takeAsList(USER_IDENTIFIER_DATASET_SIZE);     
	      logger.info("Creates the " + userIdentifierDataSetCount + " User data records based on an email address and address.");     
	      for (int e  = 0; e < userIdentiferInputList.size(); e++) {
	         // logger.printf(Level.INFO, "partitionId=%d, totCount=%d, userDatas.get(%d).UserIdentifiersCount=%d", partitionId, userIdentifierDataSetCount, userDataIndex, userDatas.get(userDataIndex).getUserIdentifiersCount());
	       	 String accountId   = userIdentiferInputList.get(e).getString(0);
	      	 String firstName   = userIdentiferInputList.get(e).getString(1);
	      	 String lastName    = userIdentiferInputList.get(e).getString(2);
	      	 String email       = userIdentiferInputList.get(e).getString(3);
	      	 String postalCode  = userIdentiferInputList.get(e).getString(4);
	      	 String countryCode = userIdentiferInputList.get(e).getString(5);
	         
	         if (nonEmpty(email)) {
	            userDatas.get(userDataIndex).addUserIdentifiers(UserIdentifier.newBuilder() 
	                                         .setHashedEmail(StringValue.of(normalizeAndHash(sha256Digest, email)))
	                                         .build());
	            userIdentifierCount++;
	            addDataOperations(offlineUserDataJobServiceClient, customerId, offlineUserDataJobResourceName,
	            offlineUserDataJobOperations,
	            userDatas,
	            userIdentifierCount,
	            userIdentifierCount,
	            userDataIndex,
	            totIdCount,
	            receivedOperationCount,                                           
	         false);
	         } else {
	            emailSkipped++;	
	            logger.info("Skipping account {} because email address is missing." , accountId);
	         }
	         if (nonEmpty(lastName) && nonEmpty(firstName)) {
	            OfflineUserAddressInfo.Builder offlineUserAddressInfoBuilder = OfflineUserAddressInfo.newBuilder()
	                                                                  .setHashedFirstName(StringValue.of(normalizeAndHash(sha256Digest, firstName)))
	                                                                .setHashedLastName(StringValue.of(normalizeAndHash(sha256Digest, lastName)));
	            
	            if (nonEmpty(postalCode))  offlineUserAddressInfoBuilder.setCountryCode(StringValue.of(postalCode)); else postalCodeSkipped++;
	            if (nonEmpty(countryCode)) offlineUserAddressInfoBuilder.setPostalCode(StringValue.of(countryCode));   else countryCodeSkipped++;
	            
	            userDatas.get(userDataIndex).addUserIdentifiers(UserIdentifier.newBuilder()
	                                .setAddressInfo(offlineUserAddressInfoBuilder.build()));
	            userIdentifierCount++;
	            addDataOperations(offlineUserDataJobServiceClient, customerId, offlineUserDataJobResourceName,
	            offlineUserDataJobOperations,
	            userDatas,
	            userIdentifierCount,
	            userIdentifierCount,
	            userDataIndex,
	            totIdCount,
	            receivedOperationCount,                                           
	            false);
	         } else {
	         addressSkipped++;	
	            logger.info("Skipping account {} because either first and last names or both missing.", accountId);
	         }
	         userIdentifierDataSetCount++;
	      
	      } // while(iterator.hasNext()) 
	      addDataOperations(offlineUserDataJobServiceClient, 
	    		            customerId, 
	    		            offlineUserDataJobResourceName,
	                        offlineUserDataJobOperations,
	                        userDatas,
	                        userIdentifierCount,
	                        userIdentifierCount,
	                        userDataIndex,
	                        totIdCount,
	                        receivedOperationCount,                                           
	                        true);
	      logger.printf(Level.INFO, "Partition:%d  => UserList contains a total of %d data operations and %d user Identifiers ",     partitionId, receivedOperationCount,     totIdCount);
	      logger.printf(Level.INFO, "Partition:%d  => Received: %d, Skip Record Count: Email:%d, Address:%d, Postal:%d, Country=%d", partitionId, userIdentifierDataSetCount, emailSkipped, addressSkipped,    postalCodeSkipped, countryCodeSkipped);
   }



   private static void addDataOperations(OfflineUserDataJobServiceClient    offlineUserDataJobServiceClient,
                                         long                               customerId, 
                                         String                             offlineUserDataJobResourceName,
                                         List<OfflineUserDataJobOperation>  offlineUserDataJobOperations,
                                         List<Builder>                      userDatas,
                                         int                                uidCount,
                                         Integer                            userIdentifierCount,
	                                     Integer                            userDataIndex,
	                                     Integer                            totIdCount,
	                                     Long                               receivedOperation,                                           
                                         boolean                            lastCall) {
   	  if (0  == uidCount % USER_IDENTIFIER_LIMIT  || (lastCall  && 0  != uidCount % USER_IDENTIFIER_LIMIT )) {
	  	logger.printf(Level.DEBUG, "Adding Identifiers to DataOperation #%d", userDataIndex);		
	  	offlineUserDataJobOperations.add(OfflineUserDataJobOperation.newBuilder()
	  			                                                    .setCreate(userDatas.get(userDataIndex))
	  			                                                    .build());		
	  	userDataIndex++;
	  }
   	  if (0  == uidCount % OFFLINE_USER_JOB_OPERATIONS_LIMIT  || (lastCall  && 0  != uidCount % OFFLINE_USER_JOB_OPERATIONS_LIMIT)) {
   		logger.printf(Level.INFO, "Uploading... uidCount=%d, lastCall=%b", uidCount, lastCall);  
	  	uploadUserListOffline(offlineUserDataJobServiceClient, customerId, offlineUserDataJobResourceName, offlineUserDataJobOperations, receivedOperation);
	  	totIdCount += getUserIdentifierCountOffline(offlineUserDataJobOperations);
	  	logger.printf(Level.INFO, "Clearing %d Data Operations containing total of %d UserIdentfiers. ", offlineUserDataJobOperations.size(), getUserIdentifierCountOffline(offlineUserDataJobOperations));
	  	offlineUserDataJobOperations.clear();
	  	userDatas = getUserDatas();
	  	logger.printf(Level.INFO, "After clearing there are %d Data Operations containing total of %d UserIdentfiers. ", offlineUserDataJobOperations.size(), getUserIdentifierCountOffline(offlineUserDataJobOperations));
	  	userDataIndex       = 0;
	  	userIdentifierCount = 0;
	  }
   }



   private static List<Builder>  getUserDatas() {
	  List<Builder>   userDatas =new ArrayList<>(OFFLINE_USER_JOB_OPERATIONS_LIMIT);
	  for (int x = 0; x < OFFLINE_USER_JOB_OPERATIONS_LIMIT; x++)    userDatas.add(UserData.newBuilder());
	  return userDatas;
   }



   private static int  getUserIdentifierCountOffline(List<OfflineUserDataJobOperation> offlineUserDataJobOperations) {
	  int idCount = 0;
	  for (OfflineUserDataJobOperation oudj : offlineUserDataJobOperations)    {
		  idCount += oudj.getCreate().getUserIdentifiersCount();
	  }
	  return idCount;
  }



   public  static void uploadUserListOffline(OfflineUserDataJobServiceClient    offlineUserDataJobServiceClient,
                                             long                               customerId, 
                                             String                             offlineUserDataJobResourceName,
                                             List<OfflineUserDataJobOperation>  offlineUserDataJobOperations,
                                             Long                               receivedOperationCount) {  
	  logger.info("Building uploadUserDataRequest");  
	  receivedOperationCount +=  Long.valueOf(offlineUserDataJobOperations.size());
	  logger.printf(Level.INFO,  "Adding %d OfflineUserDataJobOperations  to offlineUserDataJobServiceClient, offlineUserDataJobResourceName=%s", offlineUserDataJobOperations.size(), offlineUserDataJobResourceName);
	  logger.printf(Level.INFO,  "offlineUserDataJobServiceClient.getOfflineUserDataJob(resourceName)=%s", offlineUserDataJobServiceClient.toString());
	  logger.printf(Level.INFO,  "offlineUserDataJobServiceClient.isShutdown()=%b", offlineUserDataJobServiceClient.isShutdown());
	  
	  AddOfflineUserDataJobOperationsResponse response =
	     offlineUserDataJobServiceClient.addOfflineUserDataJobOperations(
	        AddOfflineUserDataJobOperationsRequest.newBuilder()
	           .setResourceName(offlineUserDataJobResourceName)
	           .setEnablePartialFailure(BoolValue.of(true))
	           .addAllOperations(offlineUserDataJobOperations)
	           .build());

	  

	   logger.printf(Level.INFO, "response.getSerializedSize()=%d", response.getSerializedSize());
	   
      // Prints the status message if any partial failure error is returned.
      // NOTE: The details of each partial failure error are not printed here, you can refer to the example HandlePartialFailure.java to learn more.
      if (response.hasPartialFailureError()) {
          logger.printf(Level.INFO,
               "Encountered %d partial failure errors while adding %d operations to the offline user "
               + "data job: '%s'. Only the successfully added operations will be executed when "
               + "the job runs.%n",
               response.getPartialFailureError().getDetailsCount(),
               offlineUserDataJobOperations.size(),
               response.getPartialFailureError().getMessage());
      } else {
      logger.printf(Level.INFO,"Successfully added %d operations to the offline user data job.%n", offlineUserDataJobOperations.size());
      }
      
      //logger.printf(Level.INFO,"Successfully added %d operations to the offline user data job.%n", offlineUserDataJobOperations.size());

   }



   /** Retrieves, checks, and prints the status of the offline user data job. */
   public static void checkJobStatus(GoogleAdsClient googleAdsClient, long customerId, String offlineUserDataJobResourceName) {
      try (GoogleAdsServiceClient googleAdsServiceClient = googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
         String query =
             String.format(
                 "SELECT offline_user_data_job.resource_name, "
                     + "offline_user_data_job.id, "
                     + "offline_user_data_job.status, "
                     + "offline_user_data_job.type, "
                     + "offline_user_data_job.failure_reason "
                     + "FROM offline_user_data_job "
                     + "WHERE offline_user_data_job.resource_name = '%s'",
                 offlineUserDataJobResourceName);
         // Issues the query and gets the GoogleAdsRow containing the job from the response.
         GoogleAdsRow googleAdsRow =
             googleAdsServiceClient
                 .search(Long.toString(customerId), query)
                 .iterateAll()
                 .iterator()
                 .next();
         OfflineUserDataJob offlineUserDataJob = googleAdsRow.getOfflineUserDataJob();
         logger.printf(Level.INFO,
             "Offline user data job ID %d with type '%s' has status: %s%n",
             offlineUserDataJob.getId().getValue(),
             offlineUserDataJob.getType(),
             offlineUserDataJob.getStatus());
         OfflineUserDataJobStatus jobStatus = offlineUserDataJob.getStatus();
         if (OfflineUserDataJobStatus.FAILED == jobStatus) {
          logger.printf(Level.INFO,"  Failure reason: %s%n", offlineUserDataJob.getFailureReason());
         } else if (OfflineUserDataJobStatus.PENDING == jobStatus
             || OfflineUserDataJobStatus.RUNNING == jobStatus) {    	  
          logger.printf(Level.INFO,
               "To check the status of the job periodically, use the following GAQL query with"
                   + " GoogleAdsService.search:%n%s%n",
               query);
         }
      }
   }



   /**
   * Prints information about the Customer Match user list.
   *
   * @param googleAdsClient the Google Ads API client.
   * @param customerId the client customer ID .
   * @param userListResourceName the resource name of the Customer Match user list.
   */
   private void printCustomerMatchUserListInfo(GoogleAdsClient  googleAdsClient, 
		                                      long             customerId, 
		                                      String           userListResourceName) {
	  
    try (GoogleAdsServiceClient googleAdsServiceClient = googleAdsClient.getLatestVersion().createGoogleAdsServiceClient()) {
      // Creates a query that retrieves the user list.
      logger.printf(Level.INFO,  "userListResourceName=%s", userListResourceName);	
      logger.info("Creates a query that retrieves the user list with resource_name={userListResourceName}.");
      String query =
          String.format(
              "SELECT user_list.name, user_list.description,  user_list.size_for_display, user_list.size_for_search " + 
              "  FROM user_list " +	  
              " WHERE user_list.resource_name = '%s'",
              userListResourceName);

      // Constructs the SearchGoogleAdsStreamRequest.
      logger.info("Constructs the SearchGoogleAdsStreamRequest for  query={}", query);
      SearchGoogleAdsStreamRequest request =  SearchGoogleAdsStreamRequest.newBuilder()
                                                                          .setCustomerId(Long.toString(customerId))
                                                                          .setQuery(query)
                                                                          .build();

      // Issues the search stream request.
      logger.info("Issues the search stream request.");
      ServerStream<SearchGoogleAdsStreamResponse> stream = googleAdsServiceClient.searchStreamCallable().call(request);

      // Gets the first and only row from the response.
      logger.info("Gets the first and only row from the response.");
      GoogleAdsRow googleAdsRow = stream.iterator().next().getResultsList().get(0);
      UserList userList         = googleAdsRow.getUserList();
      logger.printf(Level.INFO, "User list '%s' has an estimated %d users for Display and %d users for Search.%n", 
    	  userList.getResourceName(),
          userList.getSizeForDisplay().getValue(),
          userList.getSizeForSearch().getValue());
      logger.info("Reminder: It may take several hoAbout Organizations\r\n" + 
                   "The Organization resource represents an organization and is the root node in the resource hierarchy. The IAM access control policies applied on the Organization resource apply to all projects (and all resources under the project) in that organization.\r\n" + 
                   "\r\n" + 
                   "An Organization is closely associated with a Google Cloud domain account. Once an Organization resource is created for a Google Cloud domain, all projects created by members of the domain will belong to the Organization resource.\r\n" + 
                   "\r\n" + 
                   "Note: For more information about Organizations or to request to be whitelisted to use this feature, see the Organization documentation.\r\n" + 
                   "\r\n" + 
                   "This is a Beta release of the Organization resource. This feature might be changed in backward-incompatible ways and is not recommended for production use. It is not subject to any SLA or deprecation policy.urs for the user list to be populated with the users.");
    }
  }

  /**
   * Returns the result of normalizing and then hashing the string using the provided digest.
   * Private customer data must be hashed during upload, as described at
   * https://support.google.com/google-ads/answer/7474263.
   *
   * @param digest the digest to use to hash the normalized string.
   * @param s the string to normalize and hash.
   */
  private static String normalizeAndHash(MessageDigest digest, String s)
      throws UnsupportedEncodingException {
    // Normalizes by removing leading and trailing whitespace and converting all characters to
    // lower case.
    String normalized = s.trim().toLowerCase();
    // Hashes the normalized string using the hashing algorithm.
    byte[] hash = digest.digest(normalized.getBytes("UTF-8"));
    StringBuilder result = new StringBuilder();
    for (byte b : hash) {
      result.append(String.format("%02x", b));
    }

    return result.toString();
  }
  
}

