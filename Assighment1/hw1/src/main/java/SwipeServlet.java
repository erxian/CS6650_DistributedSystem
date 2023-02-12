import javax.servlet.*;
import javax.servlet.http.*;
import javax.servlet.annotation.*;
import java.io.IOException;
import java.util.Arrays;

import hw1.ResponseMsg;
import hw1.SwipeDetails;
import com.google.gson.Gson;

@WebServlet(name = "SwipeServlet", value = "/SwipeServlet")
public class SwipeServlet extends HttpServlet {
  @Override
  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws ServletException, IOException {
    response.setContentType("text/plain");
    String urlPath = request.getPathInfo();
    System.out.println(urlPath);

    // check we have a URL!
    if (urlPath == null || urlPath.isEmpty()) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      response.getWriter().write("missing paramterers");
      return;
    }

    String[] urlParts = urlPath.split("/");
    // and now validate url path and return the response status code
    // (and maybe also some value if input is valid)

    response.setStatus(HttpServletResponse.SC_OK);
    // do any sophisticated processing with urlParts which contains all the url params
    // TODO: process url params in `urlParts`
    response.getWriter().write("GET works!");

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
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("User not found: missing url");
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }
    // check we have a valid URL
    if (!isUrlValid(urlParts)) {
      response.setStatus(HttpServletResponse.SC_NOT_FOUND);
      responseMsg.setMessage("User not found: invalid url");
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
      return;
    }

    // check we have a valid Request Body
    try {
      StringBuilder sb = new StringBuilder();
      String s;
      while ((s = request.getReader().readLine()) != null) {
        sb.append(s);
      }

      if (sb == null || sb.toString().isEmpty()) {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        responseMsg.setMessage("Invalid inputs: missing request body");
        response.getOutputStream().print(gson.toJson(responseMsg));
        response.getOutputStream().flush();
        return;
      }

      SwipeDetails swipeDetails = gson.fromJson(sb.toString(), SwipeDetails.class);
      if (isValidSwipeDetails(swipeDetails)) {
        responseMsg.setMessage("Write successful");
        response.setStatus(HttpServletResponse.SC_CREATED);
      } else {
        response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        responseMsg.setMessage("Invalid inputs: incorrect request body");
      }
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
    } catch (Exception ex) {
      ex.printStackTrace();
      responseMsg.setMessage(ex.getMessage());
      response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
      response.getOutputStream().print(gson.toJson(responseMsg));
      response.getOutputStream().flush();
    }
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
