package hw1;
import org.json.JSONObject;

public class SwipeDetails {
  private String swiper;
  private String swipee;
  private String comment;

  public String getSwiper() {
    return swiper;
  }

  public String getSwipee() {
    return swipee;
  }

  public String getComment() {
    return comment;
  }

  public String toJson() {
    JSONObject json = new JSONObject();
    json.put("swiper", swiper);
    json.put("swipee", swipee);
    json.put("comment", comment);
    return json.toString();
  }
}
