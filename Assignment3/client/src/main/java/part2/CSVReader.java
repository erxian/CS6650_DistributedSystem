package part2;

import com.opencsv.CSVWriter;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import org.apache.commons.math3.stat.descriptive.DescriptiveStatistics;

public class CSVReader {
  static DescriptiveStatistics stats = new DescriptiveStatistics();
  public static final String delimiter = ",";

  public static void generateThroughput(String csvFile, String throughputFile) throws IOException{
    BufferedReader br = read(csvFile);
    String line = "";
    String[] tempArr;
    String headerLine = br.readLine();

    File f = new File(throughputFile);
    FileWriter outputfile = new FileWriter(f);
    CSVWriter writer = new CSVWriter(outputfile);

    // adding header to csv
    String[] header = { "time", "throughput" };
    writer.writeNext(header);

    // sort first
    ArrayList<Double> start_time_list = new ArrayList<>();
    while ((line = br.readLine()) != null) {
      tempArr = line.split(delimiter);
      double temp_time = Double.parseDouble(tempArr[0].replace("\"",""));
      start_time_list.add(temp_time);
    }

    Collections.sort(start_time_list);
    int ONE_SECOND = 1000;
    int counter = 0;
    int cnt = 0;
    Double first_time = start_time_list.get(0);
    for(Double temp_time : start_time_list) {
       if (temp_time - (first_time+counter*1000) > ONE_SECOND) {
         counter++;
         writer.writeNext(new String[] {String.valueOf(counter), String.valueOf(cnt)});
         cnt = 0;
       } else {
         cnt++;
       }
    }
    br.close();
    writer.close();
  }
  public static void readStatistics(String csvFile) throws IOException {
    BufferedReader br = read(csvFile);
    String line = "";
    String[] tempArr;
    String headerLine = br.readLine();
    while ((line = br.readLine()) != null) {
      tempArr = line.split(delimiter);
      stats.addValue(Double.parseDouble(tempArr[2].replace("\"","")));
    }
    br.close();
    double max = stats.getMax();
    double min = stats.getMin();
    double mean = stats.getMean();
    double median = stats.getPercentile(50);
    double ninetyNinePercentile = stats.getPercentile(99);
    System.out.println("mean response time : " + mean + " millisecs");
    System.out.println("median response time : " + median + " millisecs");
    System.out.println("p99 response time : " + ninetyNinePercentile + " millisecs");
    System.out.println("max response time : " + max + " millisecs");
    System.out.println("min response time : " + min + " millisecs");
  }
  public static BufferedReader read(String csvFile) {
    try {
      File file = new File(csvFile);
      FileReader fr = new FileReader(file);
      BufferedReader br = new BufferedReader(fr);
      return br;
    } catch (IOException ioe) {
      ioe.printStackTrace();
    }
    return null;
  }
}
