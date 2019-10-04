package mapreduce;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.map.TokenCounterMapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

public class WordCount_v2 extends Configured implements Tool {
 public int run(String[] args) throws Exception {
 Job job = Job.getInstance(getConf(), "WordCount-v2");
 job.setJarByClass(WordCount_v2.class);
 job.setMapperClass(Map.class);
 job.setCombinerClass(Reduce.class);
 job.setReducerClass(Reduce.class);
 job.setInputFormatClass(TextInputFormat.class);
 job.setOutputKeyClass(Text.class);
 job.setOutputValueClass(IntWritable.class);
 job.setOutputFormatClass(TextOutputFormat.class);

 List<String> other_args = new ArrayList<String>();
 for (int i = 0; i < args.length; ++i) {
 if ("-skip".equals(args[i])) {
	 System.out.println(args[i+1]);
 DistributedCache.addCacheFile(new Path(args[++i]).toUri(),
job.getConfiguration());
 job.getConfiguration().setBoolean("wordcount.skip.patterns", true);
 } else
 other_args.add(args[i]);
 }
 for (int i=0;i<2;i++) {
	 System.out.println(other_args.get(i));
 }
 
 
 FileInputFormat.setInputPaths(job, new Path(other_args.get(0)));
  FileOutputFormat.setOutputPath(job, new Path(other_args.get(1)));
 
 return job.waitForCompletion(true) ? 0 : 1;
 }
 
  public static void main(String[] args) throws Exception {
  System.exit(ToolRunner.run(new Configuration(), new WordCount_v2(), args));
  }
  }
