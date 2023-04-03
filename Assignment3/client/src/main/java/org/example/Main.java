package org.example;

import java.io.IOException;
import part2.CSVReader;

public class Main {
  public static void main(String[] args) throws IOException {
    CSVReader.generateThroughput("result_bak.csv", "throughput.csv");
  }
}