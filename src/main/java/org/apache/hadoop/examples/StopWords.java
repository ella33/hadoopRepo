package org.apache.hadoop.examples;

import java.util.ArrayList;
import java.util.Arrays;

public class StopWords {
  public static ArrayList<String> getList() {
    return new ArrayList<String>(Arrays.asList(
        "and",
        "or",
        "with",
        "on",
        "to",
        "under",
        "after",
        "before",
        "above",
        "below",
        "not",
        "a",
        "no",
        "yes",
        "maybe",
        "in",
        "out",
        "not"
      )
    );
  }
}