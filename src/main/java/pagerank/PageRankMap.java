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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.StringTokenizer;

public  class PageRankMap extends Mapper<Object,Text,Text,Text>{        
    private String id;
    //private float pr;
    private double pr;
    private int count;
    //private float average_pr;    
    private double average_pr;
    public void map(Object key,Text value,Context context)
        throws IOException,InterruptedException{            
        StringTokenizer str = new StringTokenizer(value.toString());
        if(str.hasMoreTokens()) {//get the title
        	id =str.nextToken();
        }
        
        if(str.hasMoreTokens()) {//get the pagerank
        	pr = Double.parseDouble(str.nextToken());
        	count = str.countTokens();
        	average_pr = pr/count;
        }

        String linkids ="&";
        while(str.hasMoreTokens()){//get the outlinks and get the pagerank value provided by this title
            String linkid = str.nextToken();
            context.write(new Text(linkid),new Text("@"+average_pr));
            linkids +=" "+ linkid;
        }       
        context.write(new Text(id), new Text(linkids));
    }
}

