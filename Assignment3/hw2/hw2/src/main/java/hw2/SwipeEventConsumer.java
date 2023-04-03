package hw2;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import com.mongodb.client.model.WriteModel;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.io.IOException;
import java.util.ArrayList;

import java.util.List;
import java.util.concurrent.CountDownLatch;

import java.util.logging.Level;
import java.util.logging.Logger;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.bson.Document;
import org.bson.conversions.Bson;

public class SwipeEventConsumer {
  private static final String EXCHANGE_NAME = "swipe";
  private static final int NUM_THREADS = 30;
  private static void batchUpdateSwipe(List<String> swipeEvents, MongoCollection<Document> collection) {
    List<WriteModel<Document>> bulkUpdates = new ArrayList<>();
    for (String swipeEvent : swipeEvents) {
      JsonObject body = parseBody(swipeEvent);
      String swiper = body.get("swiper").getAsString();
      String swipee = body.get("swipee").getAsString();
      String preference = body.get("direction").getAsString();

      Bson updateLikesDislikes = preference.equals("left")
          ? Updates.inc("numDislikes", 1)
          : Updates.inc("numLlikes", 1);

      List<Bson> updateOperations = new ArrayList<>();
      updateOperations.add(Updates.setOnInsert("user_id", swiper));
      updateOperations.add(updateLikesDislikes);

      if (preference.equals("right")) {
        updateOperations.add(Updates.push("matchList", swipee));
      }

      UpdateOneModel<Document> updateSwiper = new UpdateOneModel<>(
          Filters.eq("user_id", swiper),
          Updates.combine(updateOperations),
          new UpdateOptions().upsert(true)
      );
      bulkUpdates.add(updateSwiper);
    }

    collection.bulkWrite(bulkUpdates);
  }

  private static JsonObject parseBody(String message) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(message, JsonObject.class);
    return jsonObject;
  }

  public void consumeSwipe(long threadId, Connection connection, String queueName, MongoCollection<Document> collection) throws IOException {
    try {
      Channel channel = connection.createChannel();
      // max one message per receiver
      channel.basicQos(100); // Process ten message at a time
      System.out.println(" [*] Consumer Thread " +threadId + " waiting for messages. To exit press CTRL+C");

      final List<String> batchMessages = new ArrayList<>();

      channel.basicConsume(queueName, false, new DefaultConsumer(channel) {
        @Override
        public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
          batchMessages.add(new String(body));
          if (batchMessages.size() >= 100) {
            // batch insert into mongoDB
            batchUpdateSwipe(batchMessages, collection);
            channel.basicAck(envelope.getDeliveryTag(), true);
//            System.out.println("Callback thread ID = " + threadId + " Received '" + batchMessages.size() + "'");
            batchMessages.clear();
          }
        }
      });
    } catch (IOException ex) {
      Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public static void main(String[] argv) throws Exception {

    RMQConfig rmqConfig = new RMQConfig(
        "172.31.26.70",
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

    String uri = "mongodb://172.31.16.24:27017";
    MongoClient mongoClient = MongoClients.create(uri);
    MongoDatabase database = mongoClient.getDatabase("SwipeData");
    MongoCollection<Document> collection = database.getCollection("SwipeCollection");

    // Create an index on the "user_id" field
    collection.createIndex(Indexes.ascending("user_id"));

    final SwipeEventConsumer consumer = new SwipeEventConsumer();
    CountDownLatch completed = new CountDownLatch(NUM_THREADS);
    for (int i = 0; i < NUM_THREADS; i++) {
      // lambda runnable creation - interface only has a single method so lambda works fine
      Runnable thread =  () -> {
        long threadId = Thread.currentThread().getId();
        try {
          consumer.consumeSwipe(threadId, connection, queueName, collection);
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
