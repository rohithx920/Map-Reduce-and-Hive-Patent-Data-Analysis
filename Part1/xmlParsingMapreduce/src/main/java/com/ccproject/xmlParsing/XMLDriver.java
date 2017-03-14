package com.ccproject.xmlParsing;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

/**
 * @author Lakshmi Sridhar
 *
 */
public class XMLDriver {
	public static String filename = new String();
	public static String output_folder = new String();

	public static void main(String[] args) throws Exception {
		GenericOptionsParser parser = new GenericOptionsParser(args);
		Configuration conf = parser.getConfiguration();
		String[] arguments = parser.getRemainingArgs();

		conf.set("xmlinput.start", "<us-patent-grant");
		conf.set("xmlinput.end", "</us-patent-grant>");

		Job job = Job.getInstance(conf);
		job.setJarByClass(XMLDriver.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReducer.class);

		job.setInputFormatClass(XmlInputFormat.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);

		MultipleOutputs.addNamedOutput(job, "patentDataAttributes",
				TextOutputFormat.class, Text.class, Text.class);
		MultipleOutputs.addNamedOutput(job, "patentData",
				TextOutputFormat.class, Text.class, Text.class);

		MultipleInputs.addInputPath(job, new Path(arguments[0]),
				XmlInputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(arguments[1]));

		job.waitForCompletion(true);
	}

}