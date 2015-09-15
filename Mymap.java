package usecase1;

import java.io.IOException;
import java.util.*;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Mymap extends Mapper<LongWritable, Text, Text, IntWritable> {
 private final static IntWritable one=new IntWritable(1);
 @Override
  public void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {
	  
	  Text word=new Text();
	  String line=value.toString();
	  StringTokenizer tokens=new StringTokenizer(line);
	  while(tokens.hasMoreTokens()){
		word.set(tokens.nextToken()); 
		context.write(word, one);
	  }
	  
  }
}

