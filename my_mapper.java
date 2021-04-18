package wordcounter_pac;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.LongWritable;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;


public class my_mapper extends Mapper<LongWritable,Text,Text,IntWritable> {
	
	private Text outputKey = new Text();
	
	public void map(LongWritable key,Text value,Context context)
	throws IOException,InterruptedException
	{	
	
		
		String line = value.toString();
		String [] words = StringUtils.split(line, ' ');
		
		for(String word : words) {
			outputKey.set(word);
			context.write(outputKey,new IntWritable(1));
									}
	}
}
