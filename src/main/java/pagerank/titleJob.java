package pagerank;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import utils.ISO8601;



public class titleJob extends Configured implements Tool {
	
	static class newMap extends org.apache.hadoop.mapreduce.Mapper<LongWritable,Text, Text, Text> {
		private  Text tittle = new Text(); //the title of the record
		public void map(LongWritable key, Text value, Context context) throws
		IOException, InterruptedException {
			String line = value.toString();//value to string(input operation)
			StringTokenizer tokenizer = new StringTokenizer(line);
						
			String Stringtitle = null; 
			String time=null;
			String strstr="";

			while (tokenizer.hasMoreTokens()) {

				
				String tick =tokenizer.nextToken();

				if(tick.equals("REVISION"))//get the title of each record
				{
					tokenizer.nextToken(); //find the location of title
					tokenizer.nextToken();
					Stringtitle = tokenizer.nextToken();
					tittle.set(Stringtitle);
					time = tokenizer.nextToken(); //find the timestamp
				} 
				else if (tick.equals("MAIN")) // find all the outlinks for that title
				{
					List<String> list = new ArrayList<String>();
					String link = tokenizer.nextToken(); 
					
					while (tokenizer.hasMoreTokens() &&  !link.equals("TALK"))
					{
			            if(!list.contains(link)  && !Stringtitle.equals(link) ) { //filter the selfloops and duplicate
			            	list.add(link);
			            }
			            link = tokenizer.nextToken();
					}
					for(String str:list) {
						strstr = strstr +str+" ";
					}
					context.write(tittle, new Text(("1*"+time+"*"+strstr)));
					// 1 is the original pagerank; with *, we want to format it as "pagerank*time*outlink"
					strstr="";
						
			}					
				}
			
		}
	}

	
	static class newReduce extends Reducer<Text, Text, Text,Text> {

		public  Date usertime;
		
		public void setup(Context context) {
			
			// get the time user input and change it from String to Date
			Configuration conf = context.getConfiguration();
			String time = null;
			time=conf.get("time");
			
			try {
				this.usertime = new Date(ISO8601.toTimeMS(time));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		
		public void reduce(Text key, Iterable<Text> values, Context
				context) throws IOException, InterruptedException {

			Date selecteddate = null;
			int iii=0;
			String content = "";
			
			
			for (Text value:values) {
				String one = value.toString();
				Date ddd = null;
				
				// with this, we can get time, pagerank and outlinks
				String[] temp = one.split("\\*");
				
				try {
					 ddd = new Date(ISO8601.toTimeMS(temp[1]));
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}//transfer String to Date
				
				//Compare the time and store the latest one

				if (ddd.before(this.usertime) ) {
					
					if(iii==0) {//for the first time(no comparison, just record)
						iii = 1;
						selecteddate= ddd;
						if (temp.length==3) {//for record with outlinks
							content = temp[0]+" "+temp[2];
						}
						else if(temp.length==2){//for record outlinks is null
							content = temp[0];
						}
					}
					else {//comepare
						if(ddd.after(selecteddate)) {
							selecteddate = ddd;
							if (temp.length==3) {
								content = temp[0]+" "+temp[2];
							}
							else if(temp.length==2){
								content = temp[0];
							}
						}
					}
				}

			}
			//output the latest record
			if (iii == 1 ) {
				context.write(key, new Text(content));
			}

		}	
	}
			
 
	public int run(String[] args) throws Exception {
		String midname = args[1]+"_midfile"; //intermediate file for iteration
		//getConf().set("mapreduce.framework.name", "local");
		//getConf().set("fs.defaultFS", "file:///");
		Job job = Job.getInstance(getConf(), "WordCount_v0");
		job.setJarByClass(titleJob.class);
		
		job.setMapperClass(newMap.class);
		job.setReducerClass(newReduce.class);
		job.setInputFormatClass(MyInputFormat.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileOutputFormat.setOutputPath(job, new Path(midname));
		job.getConfiguration().set("time", args[3]); //set the time Y
		return (job.waitForCompletion(true) ? 0 : 1);
	}
	
 }