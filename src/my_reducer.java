package wordcounter_pac;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class my_reducer extends Reducer<Text, IntWritable, Text, IntWritable>{
	
	private IntWritable outputValue = new IntWritable();

	
	protected void reduce(Text key, Iterable<IntWritable> values,Context context)
			throws IOException, InterruptedException {
		
		int sum = 0;
		
		for(IntWritable count : values) {
			sum += count.get();
		}
		outputValue.set(sum);
		context.write(key, outputValue);
	}
}
