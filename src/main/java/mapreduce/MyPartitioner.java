package mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class MyPartitioner extends Partitioner<Text, IntWritable> {
 public int getPartition(Text key, IntWritable value, int numPartitions) {
 int c = Character.toLowerCase(key.toString().charAt(0));// return char [0]
 if (c < 'a' || c > 'z')
 return numPartitions - 1;
 return (int)Math.floor((float)(numPartitions - 2) * (c-'a')/('z'-'a'));
 }
 }