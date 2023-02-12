package part1;

import io.swagger.client.*;
import io.swagger.client.model.*;
import io.swagger.client.api.SwipeApi;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

public class MultiClient {
  private static final String EC2_SERVER_PATH = "http://34.219.107.11:8080/cs6650-hw1_war/";
  private static final String LOCAL_SERVER_PATH = "http://localhost:8080/cs6650_hw1_war_exploded/";
  private static int MAX_SWIPER = 5000;
  private static int MAX_SWIPEE = 1000000;
  private static final int COMMENTS_CHAR_LIMIT = 256;
  public static int TOTAL_REQUEST_NUM = 500000;

  public static final int NUM_THREADS = 1000;

  public static final int REQUEST_NUM_PER_THREAD = 5000;

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
  public void sendRequest() {
    SwipeApi apiInstance = new SwipeApi();
    RandomBody randomBody = new RandomBody();
    apiInstance.getApiClient().setBasePath(EC2_SERVER_PATH);
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
  }

  public static void main(String[] args) throws InterruptedException {
//    CSVReader.generateThroughput("result_bak.csv", "throughput.csv");
    final MultiClient counter = new MultiClient();
    CountDownLatch completed = new CountDownLatch(NUM_THREADS);

    long startTime = System.currentTimeMillis();
    for (int i = 0; i < NUM_THREADS; i++) {
      // lambda runnable creation - interface only has a single method so lambda works fine
      Runnable thread =  () -> { counter.sendRequest();
        completed.countDown();
      };
      new Thread(thread).start();
    }
    completed.await();

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
  }
}
