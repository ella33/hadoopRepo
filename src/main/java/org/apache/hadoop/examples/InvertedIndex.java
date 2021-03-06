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

    private Text word = new Text();
    private Text filename = new Text();
    private ArrayList<String> stopWords = new ArrayList<String>();

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
      Path[] localPaths = context.getLocalCacheFiles();
      if(localPaths != null && localPaths.length > 0) {
        readFile(localPaths[0]);
      }
    }

    private void readFile(Path filePath) {
      try{
        BufferedReader bufferedReader = new BufferedReader(new FileReader(filePath.toString()));
        String stopWord = null;
        while((stopWord = bufferedReader.readLine()) != null) {
          stopWords.add(stopWord.toLowerCase());
        }
      } catch(IOException ex) {
        System.err.println("Exception while reading stop words file: " + ex.getMessage());
      }
    }


    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String filenameStr = ((FileSplit) context.getInputSplit()).getPath().getName();
      filename = new Text(filenameStr);
      String[] lineData = value.toString().split("#"); 
      String lineNumber = lineData[0];
      String line = lineData[1];
      for (String token : line.split("[:\\*$\\[\\]();,\".!?\\-\\s+]")) { 
        token = token.toLowerCase().replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", "");
        if (!StringUtils.isEmpty(token) && !stopWords.contains(token)) {
          word.set(token);
          Text fileAndLine = new Text("(" + filename + "," + lineNumber + ")");
          // add to output
          context.write(word, fileAndLine);
        }  
      }
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
    if (otherArgs.length != 3) {
        System.err.println("Usage: InvertedIndex <in> <out> <stopwords>");
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
    job.addCacheFile(new Path(args[2]).toUri());
    FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
    FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}