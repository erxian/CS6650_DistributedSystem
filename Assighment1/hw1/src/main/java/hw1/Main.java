package hw1;

import com.google.gson.Gson;
import hw1.ResponseMsg;

public class Main {
    public static void main(String[] args) {
        Gson gson = new Gson();
        ResponseMsg responseMsg = new ResponseMsg();
        responseMsg.setMessage("Write successful");
        System.out.println(gson.toJson(responseMsg));
    }
}