import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class TwitterTopFollowers {

  //Mapper class
  public static class FirstMapper extends MapReduceBase implements
  Mapper<LongWritable,  /*Input key Type */
  Text,                   /*Input value Type*/
  Text,                   /*Output key Type*/
  Text>            /*Output value Type*/
  {
    //Map function
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
        String line = value.toString();
        String[] account_follower = new line.split("\t");
        String account = account_follower[0];
        String follower = account_follower[1];
        output.collect(new Text(account), new Text(follower));
        output.collect(new Text(follower), new Text(("-"+account)));
    }
  }
   
  //Reducer class
  public static class FirstReduce extends MapReduceBase implements
  Reducer< Text, Text, Text, Text >
  {
    //Reduce function
    public void reduce(Text key, Iterator <Text> values, OutputCollector<Text, Text > output, Reporter reporter) throws IOException
    {
        ArrayList<String> neg_follower_values =new ArrayList();
        ArrayList<String> follower_values =new ArrayList();
        String follower = "";
        while (values.hasNext()) {
          follower = values.next().get();
          if(follower.substring(0,1).equals("-")){
            neg_follower_values.add(follower.substring(1));
          } else {
            follower_values.add(follower);
          }
        }
        for (String i : neg_follower_values) {
          for (String i : follower_values) {
            output.collect(new Text(neg_follower_values), new Text(follower_values));
          }
        }
        for (String i : follower_values) {
          output.collect(new Text(key), new Text(follower_values));
        }
    }
  }

  //Mapper class
  public static class SecondMapper extends MapReduceBase implements
  Mapper<LongWritable,  /*Input key Type */
  Text,                   /*Input value Type*/
  Text,                   /*Output key Type*/
  Text>            /*Output value Type*/
  {
    //Map function
    public void map(LongWritable key, Text value, OutputCollector<Text, Text> output, Reporter reporter) throws IOException
    {
      String line = value.toString();
      String[] account_follower = new line.split("\t");
      String account = account_follower[0];
      String follower = account_follower[1];
      output.collect(new Text(account), new Text(follower));
    }
  }

  //Reducer class
  public static class SecondReduce extends MapReduceBase implements
  Reducer< Text, Text, Text, Text >
  {
    //Reduce function
    public void reduce(Text key, Iterator <Text> values, OutputCollector<Text, Text > output, Reporter reporter) throws IOException
    {
      int follower_number = 0;
      while (values.hasNext()) {
        follower = values.next();
        follower_number += 1;
      }

    }
  }

  //Main function

  public static void main(String args[])throws Exception
  {
    JobConf first_conf = new JobConf(Eleunits.class);
  
    first_conf.setJobName("first_iteration");
  
    first_conf.setOutputKeyClass(Text.class);
    first_conf.setOutputValueClass(Text.class);
  
    first_conf.setMapperClass(FirstMapper.class);
    first_conf.setCombinerClass(FirstReduce.class);
    first_conf.setReducerClass(FirstReduce.class);
  
    first_conf.setInputFormat(TextInputFormat.class);
    first_conf.setOutputFormat(TextOutputFormat.class);
  
    FileInputFormat.setInputPaths(first_conf, new Path(args[0]));
    FileOutputFormat.setOutputPath(first_conf, new Path(args[1]));
  
    JobClient.runJob(conf);
    
    JobConf second_conf = new JobConf(Eleunits.class);
  
    second_conf.setJobName("second_iteration");
  
    second_conf.setOutputKeyClass(Text.class);
    second_conf.setOutputValueClass(Text.class);
  
    second_conf.setMapperClass(SecondMapper.class);
    second_conf.setCombinerClass(SecondReduce.class);
    second_conf.setReducerClass(SecondReduce.class);
  
    second_conf.setInputFormat(TextInputFormat.class);
    second_conf.setOutputFormat(TextOutputFormat.class);
  
    FileInputFormat.setInputPaths(second_conf, new Path(args[1]));
    FileOutputFormat.setOutputPath(second_conf, new Path(args[2]));
  
    JobClient.runJob(conf);
  }
}