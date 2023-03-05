import com.google.gson.JsonObject;
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

@WebServlet(name = "SwipeServlet", value = "/SwipeServlet")
public class SwipeServlet extends HttpServlet {
  private static final String EXCHANGE_NAME = "swipe";
  private RMQChannelPool channelPool;

  private static final int CHANNEL_POOL_SIZE = 30;

  public void init() throws ServletException {
    super.init();
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

    Connection connection;
    try {
      connection = factory.newConnection();
    }  catch (TimeoutException | IOException e) {
      throw new RuntimeException(e);
    }
    channelPool = new RMQChannelPool(CHANNEL_POOL_SIZE, new RMQChannelFactory(connection));
  }

  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("text/plain");
    String urlPath = request.getPathInfo();

    ResponseMsg responseMsg = new ResponseMsg();

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      responseMsg.setMessage("missing paramterers");
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, responseMsg);
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)
    if (!isUrlValid(urlParts)) {
      responseMsg.setMessage("Invalid URL");
      sendErrorResponse(response, HttpServletResponse.SC_NOT_FOUND, responseMsg);
      return;
    }
    responseMsg.setMessage("GET works!");
    sendSuccessResponse(response, HttpServletResponse.SC_OK, responseMsg);
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
    return (swiper >= 1 && swiper <= 5000) && (swipee >= 1 && swipee <= 1000000) && (comment.length() == 256);
  }
}