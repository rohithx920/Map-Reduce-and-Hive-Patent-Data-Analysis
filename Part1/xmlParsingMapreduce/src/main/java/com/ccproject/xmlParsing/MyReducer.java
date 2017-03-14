package com.ccproject.xmlParsing;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

/**
 * @author Lakshmi Sridhar
 *
 */
public class MyReducer extends Reducer<Text, IntWritable, Text, NullWritable> {
    MultipleOutputs mos;
    
    protected void cleanup(Reducer<Text, IntWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException
    {
      this.mos.close();
      super.cleanup(context);
    }
    
    protected void setup(Reducer<Text, IntWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException
    {
      this.mos = new MultipleOutputs(context);
      super.setup(context);
    }
    
    public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, NullWritable>.Context context)
      throws IOException, InterruptedException
    {
      this.mos.write("patentDataAttributes", key, NullWritable.get());
    }
  }
