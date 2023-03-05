package hw2;
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
import javafx.util.Pair;
import com.google.gson.Gson;
import com.google.gson.JsonObject;

public class Consumer {
  private static final String EXCHANGE_NAME = "swipe";
  private static final int NUM_THREADS = 20;
  // swipeData = {"swiper_id": {"left": 134, "right": 21}}
  private static Map<String, Map<String, Integer>> swipeData = new HashMap<>();

  private static synchronized void updateSwipe(String swipeId, String action) {
    Map<String, Integer> data = swipeData.get(swipeId);
    if (data == null) {
      data = new HashMap<>();
      swipeData.put(swipeId, data);
    }
    Integer curr = data.getOrDefault(action, 0) + 1;
    data.put(action, curr);
    System.out.println(swipeId + " likes: " + data.get("right") + ", dislike: " + data.get("left"));
//    printSwipeData();
  }

  private Pair<String, String> parseBody(String message) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
    String swiper = jsonObject.get("swiper").getAsString();
    String action = jsonObject.get("direction").getAsString();
    return new Pair<>(swiper, action);
  }

  public void countSwipe(Connection connection, String queueName) throws IOException {
    try {
      Channel channel = connection.createChannel();
      // max one message per receiver
      channel.basicQos(1); // Process one message at a time
      System.out.println(" [*] Consumer Thread waiting for messages. To exit press CTRL+C");
      DeliverCallback deliverCallback = (consumerTag, delivery) -> {
        String message = new String(delivery.getBody(), "UTF-8");
        Pair<String, String> swipeInfo = parseBody(message);
        updateSwipe(swipeInfo.getKey(), swipeInfo.getValue());
        channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
      };
      channel.basicConsume(queueName, false, deliverCallback, consumerTag -> { });
    } catch (IOException ex) {
      Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  private static void printSwipeData() {
    int totalLikes = swipeData.values()
        .stream()
        .mapToInt(data -> data.getOrDefault("left", 0))
        .sum();

    int totalDislikes = swipeData.values()
        .stream()
        .mapToInt(data -> data.getOrDefault("right", 0))
        .sum();
    System.out.println("like number: " + totalLikes + ", dislike number: " + totalDislikes);
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

    final Consumer consumer = new Consumer();
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