package mapreduce;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

public class WordCount_v3 extends Configured implements Tool {
 public int run(String[] args) throws Exception {
 //Job job = Job.getInstance(getConf());
 Job job = Job.getInstance(getConf(), "WordCount-v3");
 //job.setJobName("MyWordCount(" + args[0] + ")");
 job.setJarByClass(WordCount_v3.class);
 job.setInputFormatClass(MyInputFormat.class);
 job.setOutputFormatClass(TextOutputFormat.class);
 job.setMapperClass(MyMapper.class);
 job.setPartitionerClass(MyPartitioner.class);
 job.setMapOutputKeyClass(Text.class);
 job.setMapOutputValueClass(IntWritable.class);
 job.setReducerClass(MyReducer.class);
 job.setCombinerClass(MyReducer.class);
 //job.setOutputFormatClass(TextOutputFormat.class);
 FileInputFormat.setInputPaths(job, new Path(args[0]));
 //FileOutputFormat.setOutputPath(job, new Path(job.getJobName() +"_output"));
  FileOutputFormat.setOutputPath(job, new Path(args[1]));


 return job.waitForCompletion(true) ? 0 : 1;
 }

 public static void main(String[] args) throws Exception {
 System.exit(ToolRunner.run(new WordCount_v3(), args));
 }
 }
