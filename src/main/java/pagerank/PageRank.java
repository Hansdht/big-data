package pagerank;

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

import mapreduce.WordCount_v0;


public class PageRank extends Configured implements Tool {
	
	public int run(String[] args) throws Exception {
		 //getConf().set("mapreduce.framework.name", "local");
		 //getConf().set("fs.defaultFS", "file:///");
		 Job job = Job.getInstance(getConf(), "WordCount_v1");
		 job.setJarByClass(PageRank.class);
		 
		 


		 job.setMapperClass(PageRankMap.class);
		 job.setReducerClass(PageRankReducer.class);
		 job.setInputFormatClass(TextInputFormat.class);
		
		 job.setOutputKeyClass(Text.class);
		 job.setOutputValueClass(Text.class);
		 
		 FileInputFormat.setInputPaths(job, new Path(args[0]));
		 FileOutputFormat.setOutputPath(job, new Path(args[1]));
		 
		 return (job.waitForCompletion(true) ? 0 : 1);
		 }
	public static void main(String[] args) throws Exception {

		String midname = args[1]+"_midfile";
		String output = args[1];

		//first step is to generate the file that can be used for Pagerank Map-reduce
		if(ToolRunner.run(new Configuration(), new titleJob(), args)==0) {
			
			
			
			String inputfile = midname;
			String outputfile;
			
			int count = Integer.parseInt(args[2]);
			
			
			for (int i=0;i<count;i++) {	// the times of iteration
				Configuration conf = new Configuration();
				//outputfile =output+"_"+i;
				
				if (i==(count-1)) {//if the loop file is the last one, the method to name it is different
					 outputfile = output;
					 conf.setBoolean("last", true);
				}
				else {
					//conf.setBoolean("last", false);
					 outputfile =output+"_"+i;
				}
				
				
				args[0]=inputfile;
				args[1]=outputfile;
				
				if(ToolRunner.run(conf, new PageRank(), args)==0) {
					inputfile = outputfile;
				}
				else {
					System.exit(1);
				}
				
				
			}
			System.exit(0);
		}
		else {
			System.exit(1);
		}
	
	}
	
		 
  }

