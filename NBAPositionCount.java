//Designed for Hadoop MapReduce
//Counts the number of players in each position
//Written by Jian An Chiang
import java.io.IOException;
import java.util.*;

import org.apache.hadoop.fs.Path; //Accesses Paths within Hadoop
import org.apache.hadoop.conf.*; //Configures Map Reduce Jobs
import org.apache.hadoop.io.*; //Input/Output in Hadoop
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NBAPositionCount {

	public static void main(String[] args)throws Exception{
		
		Configuration conf=new Configuration(); 
		Job job = Job.getInstance(conf,"NBAPositionCount");
		
		//Set Jar, Mapper, Reducer
		job.setJarByClass(NBAPositionCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		//Configuring the input/output path from the filesystem into the job.
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		 	
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	//Mapper Class inherits Super Class Mapper
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException,StringIndexOutOfBoundsException{
				String line = value.toString();
				StringTokenizer tokenizer = new StringTokenizer(line); 	
				int x=1;
				while(tokenizer.hasMoreTokens()){
					if (x==3){
						value.set(tokenizer.nextToken());	
						context.write(value, new IntWritable(1));  
					}else{
						tokenizer.nextToken();
					}
				}
				x++;
			}
		}
	}

	//Reducer Class inherits Super Class Reducer
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{  
				
		int sum=0;
		for (IntWritable x: values){ 
			sum+=x.get(); 
		}
		context.write(key, new IntWritable(sum)); 
		}		
	}
	
}
