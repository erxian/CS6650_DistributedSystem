package hw2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;


public class ConsumerPlus {
  private static final String EXCHANGE_NAME = "swipe";
  private static final int NUM_THREADS = 20;
  private static final int SWIPEE_NUM = 100;
  // swipeData = {"swiper_id": {"swipee1_id": 10, "swipee2_id": 9, "swipee3_id: 8,....}}
  private static final Map<String, Map<String, Integer>> swipeData = new HashMap<>();

  public static synchronized void recordSwipe(String swiper, String swipee) {
    Map<String, Integer> data = swipeData.get(swiper);
    if (data == null) {
      data = new HashMap<>();
      swipeData.put(swiper, data);
    }
    Integer curr = data.getOrDefault(swipee, 0) + 1;
    data.put(swipee, curr);

    List<String> swipedUsers = getTopRightSwipedUsers(swiper);
    System.out.println(swiper + ": " + swipedUsers);
  }

  public static List<String> getTopRightSwipedUsers(String swiper) {
    Map<String, Integer> userSwipes = swipeData.getOrDefault(swiper, new HashMap<>());
    // sorting the queue in descending order of right swipes.
    PriorityQueue<String> pq = new PriorityQueue<>((a, b) -> userSwipes.get(b) - userSwipes.get(a));
    for (String swipee : userSwipes.keySet()) {
      if (userSwipes.get(swipee) > 0) {
        pq.offer(swipee);
        if (pq.size() > SWIPEE_NUM) {
          // remove the lowest priority element (i.e., the user with the least number of right swipes)
          // from the priority queue pq
          pq.poll();
        }
      }
    }
    return new ArrayList<>(pq);
  }

  private JsonObject parseBody(String message) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
    return jsonObject;
  }

  public void countSwipe(Connection connection, String queueName) throws IOException {
    try {
      Channel channel = connection.createChannel();
      channel.basicQos(1); // Process one message at a time
      System.out.println(" [*] ConsumerPlus Thread waiting for messages. To exit press CTRL+C");
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
//        System.out.println(" [x] Received '" + message + "'");
        JsonObject swipeInfo = parseBody(message);

        String swiper = swipeInfo.get("swiper").getAsString();
        String swipee = swipeInfo.get("swipee").getAsString();
        String action = swipeInfo.get("direction").getAsString();

        if(action.equals("right")) {
          recordSwipe(swiper, swipee);
        }
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        System.out.println(
            "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message
                + "'");
      };
      channel.basicConsume(queueName, false, deliverCallback, consumerTag -> {
      });
    } catch (IOException ex) {
        Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
      }
  }

  public static void main(String[] argv) throws Exception {
    RMQConfig rmqConfig = new RMQConfig(
//        "172.31.26.70",
        "35.90.245.144",
        5672,
        "cherry_broker",
        "zp",
        "12345"
    );

    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost(rmqConfig.getHost());
    factory.setPort(rmqConfig.getPort());
    factory.setVirtualHost(rmqConfig.getVirtualHost());
    factory.setUsername(rmqConfig.getUsername());
    factory.setPassword(rmqConfig.getPassword());
    Connection connection = factory.newConnection();

    Channel channel = connection.createChannel();
    channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
    String queueName = channel.queueDeclare().getQueue();
    channel.queueBind(queueName, EXCHANGE_NAME, "");

    final ConsumerPlus consumer = new ConsumerPlus();
    CountDownLatch completed = new CountDownLatch(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      // lambda runnable creation - interface only has a single method so lambda works fine
      Runnable thread =  () -> {
        try {
          consumer.countSwipe(connection, queueName);
        } catch (IOException e) {
          throw new RuntimeException(e);
        }
        completed.countDown();
      };
      new Thread(thread).start();
    }
    completed.await();
  }
}
