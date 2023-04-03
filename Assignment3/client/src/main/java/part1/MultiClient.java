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

public class MultiClient {
//  private static final String EC2_SERVER_PATH = "http://SwipeLB-1967314209.us-west-2.elb.amazonaws.com/cs6650-hw1_war/";
  private static final String EC2_SERVER_PATH ="http://35.161.8.165:8080/cs6650-hw1_war";
//  private static final String LOCAL_SERVER_PATH = "http://localhost:8080/cs6650_hw1_war_exploded";
  private static int MAX_SWIPER = 50000; // 50000
  private static int MAX_SWIPEE = 5000; // 50000
  private static final int COMMENTS_CHAR_LIMIT = 256;

  public static volatile boolean postThreadRunning = true;
  public static int TOTAL_REQUEST_NUM = 500000;

  public static final int NUM_THREADS = 100;

  public static final int REQUEST_NUM_PER_THREAD = TOTAL_REQUEST_NUM/NUM_THREADS;

  static AtomicInteger success_requests = new AtomicInteger(0);
  static AtomicInteger failed_requests = new AtomicInteger(0);


  public void increaseSuccessRequests() {
    success_requests.getAndIncrement();
  }

  public static int getSuccessRequestsNum() {
    return success_requests.get();
  }

  public void increaseFailedRequests() {
    failed_requests.getAndIncrement();
  }

  public static int getFailedRequestsNum() {
    return failed_requests.get();
  }

  public boolean retryRequest(SwipeApi apiInstance, SwipeDetails body, String leftorright)
      throws ApiException {
    int retries = 0;
    int MAX_RETRIES = 5;
    boolean retry = true;
    try {
      while (retry && retries < MAX_RETRIES) {
        ApiResponse<Void> apiResponse = apiInstance.swipeWithHttpInfo(body, leftorright);
        if (apiResponse.getStatusCode() == 201) {
          increaseSuccessRequests();
          return true;
        }
      }
    } catch(ApiException e){
      System.err.println("Exception when calling SwipeApi#swipe");
      e.printStackTrace();
    }
    return false;
  }
  public void sendPostRequest() {
    SwipeApi apiInstance = new SwipeApi();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);
    RandomBody randomBody = new RandomBody();

    for (int i = 0; i <  REQUEST_NUM_PER_THREAD; i++) {
      SwipeDetails body = new SwipeDetails(); // SwipeDetails | response details
      body.setSwipee(String.valueOf(randomBody.randomNumber(MAX_SWIPEE)));
      body.setSwiper(String.valueOf(randomBody.randomNumber(MAX_SWIPER)));
      body.setComment(randomBody.randomString(COMMENTS_CHAR_LIMIT));
      String leftorright = randomBody.randomSwipe(); // random direction left or right
      try {
        ApiResponse<Void> apiResponse = apiInstance.swipeWithHttpInfo(body, leftorright);
        if (apiResponse.getStatusCode() == 201) {
          increaseSuccessRequests();
        } else {
          if (!retryRequest(apiInstance, body, leftorright)) {
            increaseFailedRequests(); // retry 5 times but failed
          }
        }
      } catch (ApiException e) {
        System.err.println("Exception when calling SwipeApi#swipe");
        e.printStackTrace();
      }
    }
  }

  private void getMatchStats(String userId) {
    StatsApi apiInstance = new StatsApi();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);
    userId = "17770";
    try {
      ApiResponse<MatchStats> apiResponse = apiInstance.matchStatsWithHttpInfo(userId);
      if (apiResponse.getStatusCode() == 200) {
        System.out.println(apiResponse.getData().toString());
      }
    } catch (ApiException e) {
//      System.err.printf("Exception when calling StatsApi#stats for userId %s, status code: %d, %s%n", userId, e.getCode(), e.getResponseBody());
    }
  }

  private void getMatches(String userId) {
    MatchesApi apiInstance = new MatchesApi();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);
    userId = "17770";
    try {
      ApiResponse<Matches> apiResponse = apiInstance.matchesWithHttpInfo(userId);
      if (apiResponse.getStatusCode() == 200) {
        System.out.println(apiResponse.getData().toString());
      }
    } catch (ApiException e) {
//      System.err.printf("Exception when calling MatchesApi#matches for userId %s, status code: %d, %s%n", userId, e.getCode(), e.getResponseBody());
    }
  }

  private void sendGetRequest(ArrayList<Long> getLatencies){
    RandomBody randomBody = new RandomBody();
    Random random = new Random();
    while(postThreadRunning) {
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
    final MultiClient counter = new MultiClient();

    // multi post thread
    CountDownLatch startPostThread = new CountDownLatch(NUM_THREADS);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < NUM_THREADS; i++) {
      // lambda runnable creation - interface only has a single method so lambda works fine
      Runnable postThread =  () -> { counter.sendPostRequest();
        startPostThread.countDown();
      };
      new Thread(postThread).start();
    }
    // A single get thread started after all the posting threads have started
    ArrayList<Long> getLatencies = new ArrayList<>();
    Runnable startGetThread = () -> counter.sendGetRequest(getLatencies);
    new Thread(startGetThread).start();

    startPostThread.await();
    long endTime = System.currentTimeMillis();
    postThreadRunning = false;  // terminate GET thread after the last posting thread terminates.

   Thread.sleep(1000);  // wait 1 second for GET thread terminate completely

    long totalTime = (endTime - startTime)/1000;
    System.out.println("success POST requests: " + getSuccessRequestsNum());
    System.out.println("failed POST requests: " + getFailedRequestsNum());
    System.out.println("wall time is: " + totalTime);
    if (totalTime > 0) {
      System.out.println("throughput is: " + TOTAL_REQUEST_NUM/totalTime);
    } else {
      System.out.println("throughput is beyond calculated");
    }
  }
}
