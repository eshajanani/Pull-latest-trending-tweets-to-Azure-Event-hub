package com.twitter.trends;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

import twitter4j.Location;
import twitter4j.ResponseList;
import twitter4j.Trends;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.conf.ConfigurationBuilder;
import twitter4j.json.DataObjectFactory;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.JSONException;
import twitter4j.JSONObject;

import java.io.IOException;
import java.nio.charset.*;
import java.nio.file.Paths;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.*;
import com.microsoft.azure.servicebus.*;
import com.microsoft.azure.eventhubs.EventData;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;


import java.util.*;


public class GetTrendingTopics {
	
	 static HashMap hm = new HashMap();

    public static void main(String[] args) throws ServiceBusException, ExecutionException, InterruptedException, IOException, JSONException, TwitterException{

    try {

       ConfigurationBuilder cb = new ConfigurationBuilder();
       cb.setDebugEnabled(true).setOAuthConsumerKey("I7UOdGOx3RD1CPr263HQBPsjY").setOAuthConsumerSecret("Obs8GlCdEJaTY12ynlnBOCCv1Njnyb8OvYo7HZDjnCai8VXipg").setOAuthAccessToken("104822901-g7JZ4xV7NftUjpvrwIQLOZ659tDj5ANnOTdtBeet").setOAuthAccessTokenSecret("tKs4nrsUwnoxqZIe6raKgRBjC7ZMlDFO81hGjslhT1KG2");

       String fileName = "timezone.txt";
       String line = "";
       String cvsSplitBy = ",";
       
       BufferedReader br = new BufferedReader(new FileReader(fileName));

       try {

           while ((line = br.readLine()) != null) {

               // use comma as separator
               String[] country = line.split(cvsSplitBy);
               
               hm.put(country[0], country[1]);

             //  System.out.println("Country [code= " + country[0] + " , name=" + country[1] + "]");

           }

       } catch (IOException e) {
           e.printStackTrace();
       }
       
       
    
        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        ResponseList<Location> locations;
        locations = twitter.getAvailableTrends();

        Integer idTrendLocation = getTrendLocationId("United States");

        if (idTrendLocation == null) {
        System.out.println("Trend Location Not Found");
        System.exit(0);
        }

        Trends trends = twitter.getPlaceTrends(idTrendLocation);
        for (int i = 0; i < 10; i++) {
        	System.out.println("&&&&&&&&& ***********************$%%%%%%%%%%$%$$$$$$$$$$$$$$$ ");
        	
        	
        System.out.println(trends.getTrends()[i].getName());
        
        System.out.println(trends.getTrends().length);
        get_feeds(trends.getTrends()[i].getName().toString(),twitter);
        
        }

        System.exit(0);

    } catch (TwitterException te) {
        te.printStackTrace();
        System.out.println("Failed to get trends: " + te.getMessage());
        System.exit(-1);
    }
    }
private static void get_feeds(String topic, Twitter twitter) throws IOException, InterruptedException, ExecutionException, ServiceBusException, JSONException{
	
	 int limit=0;
	 
	 System.out.println(" ***************** Topic **********************************************");
	        try {
	        
	            Query query = new Query(topic);
	            QueryResult result;
	            result = twitter.search(query);
	            do{
	            	limit=limit+1;
	            List<Status> tweets = result.getTweets();
	           // System.out.println(tweets.size());
	            for (Status tweet : tweets) {
	            	
	          //     System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText());
	            /*    writer.append("a"+tweet.getUser().getScreenName());
	                writer.append("\t"+tweet.getText());               
	                writer.append("\n"); */
	                
	            /*    final String namespaceName = "eshaTSAEH";
	                final String eventHubName = "----EventHubName-----";
	                final String sasKeyName = "-----SharedAccessSignatureKeyName-----";
	                final String sasKey = "---SharedAccessSignatureKey----";
	                ConnectionStringBuilder connStr = new ConnectionStringBuilder(namespaceName, eventHubName, sasKeyName, sasKey);
	                
	                */
	        String connectionString = "Endpoint=sb://eshatsaeh.servicebus.windows.net/;SharedAccessKeyName=eshaTSAManage;SharedAccessKey=/ZmqqRBZrTS6l23qNeyyhRYOXYTszceN3mZZmcYNbWk=;EntityPath=eshatsaeh";
	    	        ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder(connectionString);

	    		Gson gson = new GsonBuilder().create();
	    		EventHubClient sender = EventHubClient.createFromConnectionString(eventHubConnectionString.toString()).get();
	    		PayloadEvent payload = new PayloadEvent();
	    		payload.username=tweet.getUser().getScreenName();
	                 payload.text1= tweet.getText();
	                Date temp_date= tweet.getCreatedAt();
	                payload.topic=topic;
	                LocalDate localDate = LocalDate.now();
	                
	                System.out.println("Topic :" +topic);
	                
	                String date1 =temp_date.getDate()+"/"+localDate.getMonthValue()+"/"+localDate.getYear()+" "+temp_date.getHours();
	                payload.createdAt= date1;
	           //     System.out.println(date1); 
	                
	                payload.lang =tweet.getLang();
	                
	                
	             //   System.out.println("@" + tweet.getUser().getScreenName() + " - " + tweet.getText()  + " , " + lang  +",");
	                
	                String json = DataObjectFactory.getRawJSON(tweet);
	              //  System.out.println(json);
	                
	           //     System.out.println("%%%%%%%%%%%%%%%%%%%%%%%%%");
	                
	                JSONObject json1 = new JSONObject(json);
	              //  System.out.println(json1.getString("user")); 
	                
	                String user = json1.getString("user");
	                JSONObject json12 = new JSONObject(user);
	            //    System.out.println(json12.getString("time_zone")); 
	                
	                payload.timezone= json12.getString("time_zone");
	                
	              //  double lon = tweet.getGeoLocation().getLongitude();
	                
	                if(payload.timezone != null && hm.get(payload.timezone)!= null)
	                {
	                	String country= hm.get(payload.timezone).toString();
	                	payload.country = hm.get(payload.timezone).toString();
	                	
	                	System.out.println("TimeZone:" + payload.timezone +" , Country : " +country);
	                }
	                
	                else{
	                	payload.country=null;
	                	
	                	System.out.println(" NO Timezone ");
	                }
	                 
	                
	    		byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
				EventData sendEvent = new EventData(payloadBytes);

				sender.send(sendEvent);
	                
			
	                
	            }
	            query=result.nextQuery();
	            if(query!=null)
	            result=twitter.search(query);
	            
	            if(limit >= 3)
                	break;
	            }while(query!= null);
	           
	           
	        } catch (TwitterException te) {
	            te.printStackTrace();
	            System.out.println("Failed to search tweets: " + te.getMessage());
	            System.exit(-1);
	        }
	
	
}
    
    
    private static Integer getTrendLocationId(String locationName) {

    int idTrendLocation = 0;

    try {

        ConfigurationBuilder cb = new ConfigurationBuilder();
cb.setDebugEnabled(true).setOAuthConsumerKey("I7UOdGOx3RD1CPr263HQBPsjY").setOAuthConsumerSecret("Obs8GlCdEJaTY12ynlnBOCCv1Njnyb8OvYo7HZDjnCai8VXipg").setOAuthAccessToken("104822901-g7JZ4xV7NftUjpvrwIQLOZ659tDj5ANnOTdtBeet").setOAuthAccessTokenSecret("tKs4nrsUwnoxqZIe6raKgRBjC7ZMlDFO81hGjslhT1KG2");

        TwitterFactory tf = new TwitterFactory(cb.build());
        Twitter twitter = tf.getInstance();

        ResponseList<Location> locations;
        locations = twitter.getAvailableTrends();

        for (Location location : locations) {
        if (location.getName().toLowerCase().equals(locationName.toLowerCase())) {
            idTrendLocation = location.getWoeid();
            break;
        }
        }

        if (idTrendLocation > 0) {
        return idTrendLocation;
        }

        return null;

    } catch (TwitterException te) {
        te.printStackTrace();
        System.out.println("Failed to get trends: " + te.getMessage());
        return null;
    }

    }
}
  class PayloadEvent
{
	String username;
	String text1;
	String createdAt;
	String topic;
	String lang;
	String timezone;
	String country;
}
