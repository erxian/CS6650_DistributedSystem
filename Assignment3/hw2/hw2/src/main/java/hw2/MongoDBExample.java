package hw2;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.Updates;
import org.bson.Document;
import com.mongodb.client.MongoCursor;

import org.bson.conversions.Bson;

public class MongoDBExample {

  private static JsonObject getStatsInfo(String document) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(document, JsonObject.class);

    int numDislikes;
    int numLikes;

    // Check if numDislikes or numLikes exists in the input JSON
    numDislikes = jsonObject.has("numDislikes") == true ? jsonObject.get("numDislikes").getAsInt() : 0;
    numLikes = jsonObject.has("numLikes") == true ? jsonObject.get("numLikes").getAsInt() : 0;

    // Create a new JsonObject with the required fields
    JsonObject newJsonObject = new JsonObject();
    newJsonObject.addProperty("numDislikes", numDislikes);
    newJsonObject.addProperty("numLikes", numLikes);

    System.out.println(newJsonObject);
    return newJsonObject;
  }

  public static void main(String[] args) {
    // Replace the connection string with your MongoDB instance's URI
    String uri = "mongodb://54.191.196.113:27017";
    MongoClient mongoClient = MongoClients.create(uri);
    MongoDatabase database = mongoClient.getDatabase("SwipeData");
    MongoCollection<Document> collection = database.getCollection("SwipeCollection");

    Document query = new Document("user_id", "17770");

    Document document = collection.find(query).first();

    if (document != null) {
      JsonObject status = getStatsInfo(document.toJson());
      System.out.println(document.toJson());
    } else {
      // Handle the case when there's no matching document (if needed)
      System.out.println("user not found");
    }

//    // Query the document by swiper_id
//    MongoCursor<Document> cursor = collection.find(query).iterator();
//
//    try {
//      // Iterate through the results and print the document contents
//      while (cursor.hasNext()) {
//        Document result1 = cursor.next();
//        System.out.println("Document content: " + result1.toJson());
//      }
//    } finally {
//      // Close the cursor and the MongoDB connection
//      cursor.close();
////      database.drop();
//      mongoClient.close();
//    }


//    // Find all documents in the collection
//    FindIterable<Document> documents = collection.find();
//
//    // Iterate through the documents and print each one
//    MongoCursor<Document> cursor = documents.iterator();
//    while (cursor.hasNext()) {
//      Document document = cursor.next();
//      System.out.println(document.toJson());
//    }
//
//    // Close the cursor
//    cursor.close();
//    // Close the client
//    mongoClient.close();

//    while(true) {
//      try {
//        for (int i = 0; i < 5; i++) {
//          long start = System.currentTimeMillis();
//
//          // Get the number of documents in the collection
//          long documentCount = collection.countDocuments();
//
//          // Print the number of documents
//          System.out.println("Number of documents: " + documentCount);
//
//          long end = System.currentTimeMillis();
//          long elapsedTime = end - start;
//
//          // Sleep for 200 milliseconds minus the elapsed time (1/5 second)
//          long sleepTime = Math.max(200 - elapsedTime, 0);
//          Thread.sleep(sleepTime);
//        }
//      } catch (InterruptedException e) {
//        e.printStackTrace();
//      } finally {
//        // Close the client
////        mongoClient.close();
//      }
//    }

//    String swipee = "28428";
//    String swiper = "47170";
//    String action = "right";
//    Bson updateLikesDislikes = action.equals("left")
//        ? Updates.inc("numDislikes", 1)
//        : Updates.inc("numLikes", 1);
//
//    collection.updateOne(
//        Filters.eq("user_id", swipee),
//        Updates.combine(
//            Updates.setOnInsert("user_id", swipee),
//            updateLikesDislikes
//        ),
//        new UpdateOptions().upsert(true)
//    );
//
//    collection.updateOne(
//        Filters.eq("user_id", swiper),
//        Updates.combine(
//            Updates.setOnInsert("user_id", swiper),
//            Updates.push("matchList", swipee)
//        ),
//        new UpdateOptions().upsert(true)
//    );

  }
}
