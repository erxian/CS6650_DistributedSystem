package part1;

import io.swagger.client.*;
import io.swagger.client.api.MatchesApi;
import io.swagger.client.api.StatsApi;
import io.swagger.client.model.*;
import io.swagger.client.api.SwipeApi;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.Random;

public class ClientGetTest {
  //  private static final String EC2_SERVER_PATH = "http://SwipeLB-1967314209.us-west-2.elb.amazonaws.com/cs6650-hw1_war/";
  private static final String EC2_SERVER_PATH ="http://35.91.73.44:8080/cs6650-hw1_war";
  //  private static final String LOCAL_SERVER_PATH = "http://localhost:8080/cs6650_hw1_war_exploded";
  private static int MAX_SWIPER = 50000; // 50000

  public static volatile boolean postThreadRunning = true;

  private void getMatchStats(String userId) {
    StatsApi apiInstance = new StatsApi();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);

    String userID = "654321"; // String | user to return matches for
    try {
      ApiResponse<MatchStats> apiResponse = apiInstance.matchStatsWithHttpInfo(userID);
      if (apiResponse.getStatusCode() == 200) {
        System.out.println("Stats: " + apiResponse.getData().toString());
      }
    } catch (ApiException e) {
      System.err.println("Exception when calling StatsApi#matches, " + "status code: " + e.getCode() + ", " + e.getResponseBody());
//      e.printStackTrace();
    }
  }

  private void getMatches(String userId) {
    MatchesApi apiInstance = new MatchesApi();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);

    String userID = "654321"; // String | user to return matches for
    try {
      ApiResponse<Matches> apiResponse = apiInstance.matchesWithHttpInfo(userID);
      if (apiResponse.getStatusCode() == 200) {
        System.out.println("matchList: " + apiResponse.getData().toString());
      }
    } catch (ApiException e) {
      System.err.println("Exception when calling MatchesApi#matches, "  + "status code: " + e.getCode() + ", " + e.getResponseBody());
//      e.printStackTrace();
    }
  }

  private void sendGetRequest(ArrayList<Long> getLatencies){
    RandomBody randomBody = new RandomBody();
    Random random = new Random();
    try {
      for (int i = 0; i < 5; i++) {
        String userId = String.valueOf(randomBody.randomNumber(MAX_SWIPER));
        String api = random.nextBoolean() ? "matches" : "stats";
        long start = System.currentTimeMillis();
        if (api.equals("matches")) {
          getMatches(userId);
        } else if (api.equals("stats")) {
          getMatchStats(userId);
        }
        long end = System.currentTimeMillis();
        long latency = end - start;
        long sleepTime = Math.max(200 - latency, 0);
        getLatencies.add(latency);
        Thread.sleep(sleepTime);
      }
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    // Compute and print the min, mean, and max latencies
    if (!getLatencies.isEmpty()) {
      long min = Collections.min(getLatencies);
      long max = Collections.max(getLatencies);
      double mean = getLatencies.stream().mapToLong(Long::longValue).average().orElse(0);

      System.out.println("Min GET latency: " + min);
      System.out.println("Mean GET latency: " + mean);
      System.out.println("Max GET latency: " + max);
    }
  }

  public static void main(String[] args) throws InterruptedException {
    final ClientGetTest counter = new ClientGetTest();

    // A single get thread started after all the posting threads have started
    ArrayList<Long> getLatencies = new ArrayList<>();
    Runnable startGetThread = () -> counter.sendGetRequest(getLatencies);
    new Thread(startGetThread).start();
  }
}
