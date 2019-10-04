package mapreduce;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MyMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
 static enum Counters { NUM_RECORDS, NUM_LINES, NUM_BYTES }
 private Text _key = new Text();
 private IntWritable _value = new IntWritable();

 protected void map(LongWritable key, Text value, Context context) throws
IOException, InterruptedException {
 StringTokenizer tokenizer = new StringTokenizer(value.toString(), "\n");
 while (tokenizer.hasMoreTokens()) {
 String line = tokenizer.nextToken();
 int sep = line.indexOf(' ');
 _key.set((sep == -1) ? line : line.substring(0, line.indexOf(' ')));
 _value.set(1);
 context.write(_key, _value);
 context.getCounter(Counters.NUM_LINES).increment(1);
 }
 context.getCounter(Counters.NUM_BYTES).increment(value.getLength());
 context.getCounter(Counters.NUM_RECORDS).increment(1);
 }
 }
