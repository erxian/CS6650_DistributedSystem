package part2;

import com.opencsv.CSVWriter;
import io.swagger.client.*;
import io.swagger.client.model.*;
import io.swagger.client.api.SwipeApi;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import part1.RandomBody;

public class MultiClientPerf {
  private static final String EC2_SERVER_PATH = "http://35.89.81.244:8080/cs6650-hw1_war/";
  private static final String LOCAL_SERVER_PATH = "http://localhost:8080/cs6650_hw1_war_exploded/";
  private static int MAX_SWIPER = 5000;
  private static int MAX_SWIPEE = 1000000;
  private static final int COMMENTS_CHAR_LIMIT = 256;
  private static final int NUM_THREADS = 100;

  private static final int REQUEST_NUM_PER_THREAD = 5000;

  private static int TOTAL_REQUEST_NUM = 500000;

  static AtomicInteger success_requests = new AtomicInteger(0);
  static AtomicInteger failed_requests = new AtomicInteger(0);

  private void increaseSuccessRequests() {
    success_requests.getAndIncrement();
  }

  private static int getSuccessRequestsNum() {
    return success_requests.get();
  }

  private void increaseFailedRequests() {
    failed_requests.getAndIncrement();
  }

  private static int getFailedRequestsNum() {
    return failed_requests.get();
  }

  private static class PerfData {
    private String start_time;
    private String request_type;
    private String latency;
    private String response_code;

    public PerfData(String start_time, String request_type, String latency, String response_code) {
      this.start_time = start_time;
      this.request_type = request_type;
      this.latency = latency;
      this.response_code = response_code;
    }

    private String[] toStringArray() {
      return new String[] {this.start_time, this.request_type, this.latency, this.response_code};
    }
  }

  private boolean retryRequest(SwipeApi apiInstance, SwipeDetails body, String leftorright)
      throws ApiException {
    System.out.println("Start Retry");
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

  private static synchronized void writeToCSV(CSVWriter writer, List<PerfData> perfDataList) {
    for (PerfData perfData : perfDataList) {
      writer.writeNext(perfData.toStringArray());
    }
  }

  public void sendRequest(CSVWriter writer) {
    SwipeApi apiInstance = new SwipeApi();
    RandomBody randomBody = new RandomBody();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);
    List<PerfData> perfDataList = new ArrayList<>();
    while (getSuccessRequestsNum() < (TOTAL_REQUEST_NUM-NUM_THREADS)) {
      for (int i = 0; i < REQUEST_NUM_PER_THREAD; i++) {
        if (getSuccessRequestsNum() > (TOTAL_REQUEST_NUM - NUM_THREADS)) {
          break;
        }
        SwipeDetails body = new SwipeDetails(); // SwipeDetails | response details
        body.setSwipee(String.valueOf(randomBody.randomNumber(MAX_SWIPEE)));
        body.setSwiper(String.valueOf(randomBody.randomNumber(MAX_SWIPER)));
        body.setComment(randomBody.randomString(COMMENTS_CHAR_LIMIT));
        String leftorright = randomBody.randomSwipe(); // random direction left or right
        try {
          long before = System.currentTimeMillis();
          ApiResponse<Void> apiResponse = apiInstance.swipeWithHttpInfo(body, leftorright);
          long after = System.currentTimeMillis();
          long latency_per_post = after - before;
          PerfData perfData = new PerfData(String.valueOf(before),
              "POST",
              String.valueOf(latency_per_post),
              String.valueOf(apiResponse.getStatusCode()));
          // store perfData to an array list
          perfDataList.add(perfData);
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
    writeToCSV(writer, perfDataList);
  }

  public static void main(String[] args) throws InterruptedException, IOException {

    final part2.MultiClientPerf counter = new part2.MultiClientPerf();
    CountDownLatch  completed = new CountDownLatch(NUM_THREADS);

    File file = new File("result.csv");
    FileWriter outputfile = new FileWriter(file);
    CSVWriter writer = new CSVWriter(outputfile);

    // adding header to csv
    String[] header = { "start_time", "request_type", "latency", "response_code" };
    writer.writeNext(header);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < NUM_THREADS; i++) {
      // lambda runnable creation - interface only has a single method so lambda works fine
      Runnable thread =  () -> { counter.sendRequest(writer);
        completed.countDown();
      };
      new Thread(thread).start();
    }

    completed.await();
    writer.close();
    long endTime = System.currentTimeMillis();
    long totalTime = (endTime - startTime)/1000;

    System.out.println("success request: " + getSuccessRequestsNum());
    System.out.println("failed request: " + getFailedRequestsNum());
    System.out.println("wall time is: " + totalTime);
    if (totalTime > 0) {
      System.out.println("throughput is: " + TOTAL_REQUEST_NUM/totalTime);
    } else {
      System.out.println("throughput is beyond calculated");
    }
    // calculate mean/median/max/min.p99
    CSVReader.readStatistics("result.csv");
    // save throughput over time
    CSVReader.generateThroughput("result.csv", "throughput.csv");
  }
}