package pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public  class PageRankReducer extends Reducer<Text,Text,Text,Text>{
	
	//private boolean last = false;
    public void reduce(Text key,Iterable<Text> values,Context context)
        throws IOException,InterruptedException{            
        String lianjie = "";
        
        double pr = 0;
        Configuration conf = context.getConfiguration();

        for(Text val:values){
        	
                if(val.toString().substring(0,1).equals("@")){
                	//calculate the pagerank of this title by adding the provided pagerank from other titles
                	pr += Double.parseDouble(val.toString().substring(1));

                }
                // summarize the outlinks of this title
                else if(val.toString().substring(0,1).equals("&")){
                    lianjie += val.toString().substring(1);
                }


        }
        pr = 0.85d*pr + 0.15d;  //use the equation
        String result;
    	result = pr+lianjie;

        
        if(conf.getBoolean("last", false)) {// if this is the last output file, just output the pagerank 
        	 
        	 result = pr+"";
        }
        else {
        	result = pr+lianjie;
        }
        
        
        context.write(key, new Text(result));           
    }
}