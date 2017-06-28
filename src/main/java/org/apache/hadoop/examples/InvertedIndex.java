package org.apache.hadoop.examples;

import java.util.HashSet;
import java.util.ArrayList;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;
import java.net.URI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.commons.lang.StringUtils;

public class InvertedIndex {

  public static class InvertedIndexMapper extends Mapper<Object, Text, Text, Text>{

    //private IntWritable lineNumber = new IntWritable(1);
    private Text word = new Text();
    private Text filename = new Text();
    private ArrayList<String> stopWords = new ArrayList<String>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      stopWords = StopWords.getList();
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
      filename = new Text(filenameStr);
      //StringTokenizer itr = new StringTokenizer(value.toString());
      String[] lineData = value.toString().split("#"); 
      String lineNumber = lineData[0];
      String line = lineData[1];
      for (String token : line.split("[:\\*$\\[\\]#();,'\".!?\\-\\s+]")) {  
        if (!StringUtils.isEmpty(token.toLowerCase()) && !stopWords.contains(token.toLowerCase())) {
          word.set(token.toLowerCase());
          // add the line number for the current file
          Text fileAndLine = new Text("(" + filename + "," + lineNumber + ")");
          // add to output
          context.write(word, fileAndLine);
        }  
      }
      // increase the line number after a line was processed  
      //lineNumber.set(lineNumber.get() + 1);
    }
  }

  public static class InvertedIndexReducer extends Reducer<Text,Text,Text,Text> {

    public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
      HashSet<String> set = new HashSet<String>();
      for (Text value : values) {
        set.add(value.toString());
      }
      StringBuilder builder = new StringBuilder();
      String prefix = "";
      for (String value : set) {
        builder.append(prefix);
        prefix = ", ";
        builder.append(value);
      }
      context.write(key, new Text(builder.toString()));
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    if (otherArgs.length != 2) {
        System.err.println("Usage: InvertedIndex <in> <out>");
        System.exit(2);
    }
    Job job = new Job(conf, "invertedIndex");
    job.setJarByClass(InvertedIndex.class);
    job.setMapperClass(InvertedIndexMapper.class);
    job.setCombinerClass(InvertedIndexReducer.class);
    job.setReducerClass(InvertedIndexReducer.class);
    job.setInputFormatClass(NLinesInputFormat.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}