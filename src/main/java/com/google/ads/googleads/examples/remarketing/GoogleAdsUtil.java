//Copyright 2020 Google LLC
//
//Licensed under the Apache License, Version 2.0 (the "License");
//you may not use this file except in compliance with the License.
//You may obtain a copy of the License at
//
//  https://www.apache.org/licenses/LICENSE-2.0
//
//Unless required by applicable law or agreed to in writing, software
//distributed under the License is distributed on an "AS IS" BASIS,
//WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//See the License for the specific language governing permissions and
//limitations under the License.


package com.google.ads.googleads.examples.remarketing;

import com.google.ads.googleads.lib.GoogleAdsClient;
import com.google.auth.Credentials;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.Level;

public class GoogleAdsUtil {
	private static final Logger logger = LogManager.getLogger(GoogleAdsUtil.class);
	private static File propertiesFile = new File("");

	
	/**
	* Creates Google Ads Credentials using settings specified in Properties file.
	*
	*/	
	public static GoogleAdsClient getGoogleAdsClient() {
		 /*  ----------   Google Ads Credentials from properties File */
		  GoogleAdsClient googleAdsClient = null;
		  try {
		   googleAdsClient = GoogleAdsClient.newBuilder().fromPropertiesFile(propertiesFile).build();
		 } catch (FileNotFoundException fnfe) {
		   System.err.printf(
		       "Failed to load GoogleAdsClient configuration from file. Exception: %s%n", fnfe);
		   System.exit(1);
		 } catch (IOException ioe) {
		   System.err.printf("Failed to create GoogleAdsClient. Exception: %s%n", ioe);
		   System.exit(1);
		 }
		 return googleAdsClient; 
	}
	
	public static Long getLoginId(GoogleAdsClient googleAdsClient) {
		return googleAdsClient.getLoginCustomerId();
	}
		  
		  




}


