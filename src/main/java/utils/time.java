package utils;

import java.util.Date;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;

import pagerank.titleJob;

public class time {
	public static void main(String[] args) throws Exception {
		String t="2013-10-12T13:50:00Z";
		String t1="2013-10-12T13:45:00Z";
		Long tt=ISO8601.toTimeMS(t);
		Long t2=ISO8601.toTimeMS(t1);
		Date d1 = new Date(tt);
		Date d2 = new Date(t2);
		System.out.println(d1.before(d2));
		System.out.println(d1);
	}
}
