import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;
import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;

import hw1.ResponseMsg;
import hw1.SwipeDetails;
import com.google.gson.Gson;

import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;


import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.json.JSONObject;


@WebServlet(name = "SwipeServlet")
public class SwipeServlet extends HttpServlet {
  private static final String EXCHANGE_NAME = "swipe";
  private RMQChannelPool channelPool;

  private MongoCollection<Document> collection;
  private static final int CHANNEL_POOL_SIZE = 30;

  public void init() throws ServletException {
    super.init();
    RMQConfig rmqConfig = new RMQConfig(
        "172.31.26.70",
//        "34.211.203.6",
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

    Connection connection;
    try {
      connection = factory.newConnection();
    }  catch (TimeoutException | IOException e) {
      throw new RuntimeException(e);
    }
    channelPool = new RMQChannelPool(CHANNEL_POOL_SIZE, new RMQChannelFactory(connection));

//    String uri = "mongodb://34.218.222.176:27017";
    String uri = "mongodb://172.31.16.24:27017";

    MongoClient mongoClient = MongoClients.create(uri);
    MongoDatabase database = mongoClient.getDatabase("SwipeData");
    collection = database.getCollection("SwipeCollection");
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");

    String requestURI = request.getRequestURI();
//    System.out.println("requestURI: " + requestURI);

    ResponseMsg responseMsg = new ResponseMsg();

    String[] urlParts = requestURI.split("/");

//    System.out.println(urlParts[2] + " " + urlParts[3]);
    // Check if the request URI matches the expected patterns
    if (!urlParts[2].equals("matches") && !urlParts[2].equals("stats")) {
      responseMsg.setMessage("Invalid inputs");
      sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, responseMsg);
      return;
    }

    // check if the request User exists
    Document query = new Document("user_id", urlParts[3]);
    long count = collection.countDocuments(query);
    if (count == 0) {
      responseMsg.setMessage("User not found");
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, responseMsg);
      return;
    }

    // retrieve data from mongodb
    Document document = collection.find(query).first();
    // Process the document
    String message;
    if (urlParts[2].equals("stats")) {
      JsonObject status = getStatsInfo(document.toJson());
      response.setStatus(HttpServletResponse.SC_OK);
      response.getWriter().write(status.toString());
    } else if (urlParts[2].equals("matches")) {
      List<String> list = getMatchesInfo(document.toJson());;
      response.setStatus(HttpServletResponse.SC_OK);
      JSONObject result = new JSONObject();
      result.put("matchList", list);
      response.getWriter().write(result.toString());
    }
  }

  private List<String> getMatchesInfo(String document) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(document, JsonObject.class);

    if (jsonObject.has("matchList") == false) {
      return new ArrayList<>();
    }

    JsonArray matchList = jsonObject.getAsJsonArray("matchList");

    // Count the frequency of each item in the matchList
    Map<String, Integer> frequencyMap = new HashMap<>();
    for (int i = 0; i < matchList.size(); i++) {
      String item = matchList.get(i).getAsString();
      frequencyMap.put(item, frequencyMap.getOrDefault(item, 0) + 1);
    }

    // Sort the frequency map entries by value in descending order
    List<Map.Entry<String, Integer>> sortedEntries = new ArrayList<>(frequencyMap.entrySet());
    sortedEntries.sort((entry1, entry2) -> entry2.getValue().compareTo(entry1.getValue()));

    // Get the top 3 most frequent items
    List<String> topThreeItems = new ArrayList<>();
    for (int i = 0; i < 10 && i < sortedEntries.size(); i++) {
      topThreeItems.add(sortedEntries.get(i).getKey());
    }
    return topThreeItems;
  }

  private JsonObject getStatsInfo(String document) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(document, JsonObject.class);

    int numDislikes;
    int numLlikes;

    // Check if numDislikes or numLlikes exists in the input JSON
    numDislikes = jsonObject.has("numDislikes") == true ? jsonObject.get("numDislikes").getAsInt() : 0;
    numLlikes = jsonObject.has("numLlikes") == true ? jsonObject.get("numLlikes").getAsInt() : 0;

    // Create a new JsonObject with the required fields
    JsonObject newJsonObject = new JsonObject();
    newJsonObject.addProperty("numDislikes", numDislikes);
    newJsonObject.addProperty("numLlikes", numLlikes);

    System.out.println(newJsonObject);
    return newJsonObject;
  }

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("application/json");
    response.setCharacterEncoding("UTF-8");

    String urlPath = request.getPathInfo();
    String[] urlParts = urlPath.split("/");

    ResponseMsg responseMsg = new ResponseMsg();
    Gson gson = new Gson();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty() || urlParts.length == 0) {
      responseMsg.setMessage("User not found: missing url");
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, responseMsg);
      return;
    }

    // check we have a valid URL
    if (!isUrlValid(urlParts)) {
      responseMsg.setMessage("User not found: invalid url");
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, responseMsg);
      return;
    }

    // Parse the request body
    SwipeDetails swipeDetails = parseRequestBody(request, gson, response, responseMsg);
    if (swipeDetails == null) {
      return;
    }

    // Validate the swipe details
    if (!isValidSwipeDetails(swipeDetails)) {
      responseMsg.setMessage("Invalid inputs: incorrect request body");
      sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, responseMsg);
      return;
    }

    Channel channel = null;
    try {
      channel = channelPool.borrowObject();
      channel.exchangeDeclare(EXCHANGE_NAME, "fanout");
      String message = generatePayload(swipeDetails, urlParts[1]);
      channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
      // Send the success response
      responseMsg.setMessage("Write successful");
      sendSuccessResponse(response, HttpServletResponse.SC_CREATED, responseMsg);
    }  catch (IOException e) {
      throw new RuntimeException(e);
    } finally {
      try {
        channelPool.returnObject(channel);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  private String generatePayload(SwipeDetails swipeDetails, String direction) {
    Gson gson = new Gson();
    JsonObject jsonObject = gson.fromJson(swipeDetails.toJson(), JsonObject.class);
    jsonObject.addProperty("direction", direction);
    String newJsonString = gson.toJson(jsonObject);
    return newJsonString;
  }
  private SwipeDetails parseRequestBody(HttpServletRequest request, Gson gson,
      HttpServletResponse response, ResponseMsg responseMsg)
      throws IOException {
    StringBuilder sb = new StringBuilder();
    String s;
    while ((s = request.getReader().readLine()) != null) {
      sb.append(s);
    }

    if (sb == null || sb.toString().isEmpty()) {
      responseMsg.setMessage("Invalid inputs: missing request body");
      sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, responseMsg);
      return null;
    }

    try {
      return gson.fromJson(sb.toString(), SwipeDetails.class);
    } catch (Exception ex) {
      ex.printStackTrace();
      responseMsg.setMessage("Invalid inputs: incorrect request body");
      sendErrorResponse(response, HttpServletResponse.SC_BAD_REQUEST, responseMsg);
      return null;
    }
  }

  private void sendErrorResponse(HttpServletResponse response, int statusCode, ResponseMsg responseMsg)
      throws IOException {
    response.setStatus(statusCode);
    response.getOutputStream().print(new Gson().toJson(responseMsg));
    response.getOutputStream().flush();
  }

  private void sendSuccessResponse(HttpServletResponse response, int statusCode, ResponseMsg responseMsg)
      throws IOException {
    response.setStatus(statusCode);
    response.getOutputStream().print(new Gson().toJson(responseMsg));
    response.getOutputStream().flush();
  }

  private boolean isUrlValid(String[] urlParts) {
    // urlPath  = "/swipe/left" or urlPath = "/swipe/right"
    // urlParts = [, left] or urlParts = [, right]
    return urlParts[1].equals("left") || urlParts[1].equalsIgnoreCase("right");
  }

  private boolean isValidSwipeDetails(SwipeDetails swipeDetails) {
    Integer swiper =  Integer.parseInt(swipeDetails.getSwiper());
    Integer swipee = Integer.parseInt(swipeDetails.getSwipee());
    String comment = swipeDetails.getComment();
    return (swiper >= 1 && swiper <= 50000) && (swipee >= 1 && swipee <= 50000) && (comment.length() == 256);
  }
}