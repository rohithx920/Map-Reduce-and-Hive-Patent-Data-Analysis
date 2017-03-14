package com.ccproject.xmlParsing;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

/**
 * @author Lakshmi Sridhar
 *
 */
public class XmlInputFormat extends TextInputFormat {
	public static final String START_TAG_KEY = "xmlinput.start";
	public static final String END_TAG_KEY = "xmlinput.end";

	public RecordReader<LongWritable, Text> createRecordReader(
			InputSplit split, TaskAttemptContext context) {
		return new XmlRecordReader();
	}

	public static class XmlRecordReader extends
			RecordReader<LongWritable, Text> {
		private byte[] startTag;
		private byte[] endTag;
		private long start;
		private long end;
		private FSDataInputStream fsin;
		private DataOutputBuffer buffer = new DataOutputBuffer();
		private LongWritable key = new LongWritable();
		private Text value = new Text();

		public void initialize(InputSplit split, TaskAttemptContext context)
				throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			this.startTag = conf.get("xmlinput.start").getBytes("utf-8");
			this.endTag = conf.get("xmlinput.end").getBytes("utf-8");
			FileSplit fileSplit = (FileSplit) split;

			this.start = fileSplit.getStart();
			this.end = (this.start + fileSplit.getLength());
			Path file = fileSplit.getPath();
			FileSystem fs = file.getFileSystem(conf);
			this.fsin = fs.open(fileSplit.getPath());
			this.fsin.seek(this.start);
		}

		public boolean nextKeyValue() throws IOException, InterruptedException {
			if ((this.fsin.getPos() < this.end)
					&& (readUntilMatch(this.startTag, false))) {
				try {
					this.buffer.write(this.startTag);
					if (readUntilMatch(this.endTag, true)) {
						this.key.set(this.fsin.getPos());
						this.value.set(this.buffer.getData(), 0,
								this.buffer.getLength());
						return true;
					}
				} finally {
					this.buffer.reset();
				}
				this.buffer.reset();
			}
			return false;
		}

		public LongWritable getCurrentKey() throws IOException,
				InterruptedException {
			return this.key;
		}

		public Text getCurrentValue() throws IOException, InterruptedException {
			return this.value;
		}

		public void close() throws IOException {
			this.fsin.close();
		}

		public float getProgress() throws IOException {
			return (float) (this.fsin.getPos() - this.start)
					/ (float) (this.end - this.start);
		}

		private boolean readUntilMatch(byte[] match, boolean withinBlock)
				throws IOException {
			int i = 0;
			do {
				int b = this.fsin.read();
				if (b == -1) {
					return false;
				}
				if (withinBlock) {
					this.buffer.write(b);
				}
				if (b == match[i]) {
					i++;
					if (i >= match.length) {
						return true;
					}
				} else {
					i = 0;
				}
			} while ((withinBlock) || (i != 0)
					|| (this.fsin.getPos() < this.end));
			return false;
		}
	}
}