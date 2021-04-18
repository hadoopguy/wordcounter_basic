package wordcounter_pac;

import org.apache.hadoop.conf.Configuration;
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

public class my_job extends Configured implements Tool {
	
	public int run(String args[]) throws Exception
	{
		Job job = Job.getInstance(getConf(),"my_job");
		
		Configuration conf = job.getConfiguration();
		
		job.setJarByClass(getClass());
		
		Path in = new Path(args[0]);
		Path out = new Path(args[1]);
		
		FileInputFormat.setInputPaths(job,in);
		FileOutputFormat.setOutputPath(job,out);
		
		job.setMapperClass(my_mapper.class);
		job.setReducerClass(my_reducer.class);
		
			
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		
		return job.waitForCompletion(true)?0:1;
	}
	
	public static void main(String[] args) {
		int result = 0;
		try {
			result = ToolRunner.run(new Configuration(), 
							new my_job(),
							args);
		} catch (Exception e) {
			e.printStackTrace();
		}
		System.exit(result);
	}

}
