package part1;

import java.util.Random;

public class RandomBody {
  public static String randomSwipe() {
    Random random = new Random();
    return random.nextInt(2) == 0 ? "left" : "right";
  }

  public static int randomNumber(int MAX_NUMBER) {
    Random random = new Random();
    return random.nextInt(MAX_NUMBER) + 1;
  }

  public static String randomString(int targetStringLength) {
    int leftLimit = 97; // letter 'a'
    int rightLimit = 122; // letter 'z'
    Random random = new Random();

    String generatedString = random.ints(leftLimit, rightLimit + 1)
        .limit(targetStringLength)
        .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
        .toString();
    return generatedString;
  }
}
