//Designed for Hadoop MapReduce
//Lists starting letters of each word and the number of occurences for each in a data file.
//This code was slightly modified from the Hadoop WordCount code. 
//Written by Jian An Chiang - First MapReduce Program
import java.io.IOException;
import java.util.*;

//All these packages are present in hadoop-common.jar
import org.apache.hadoop.fs.Path; //Accesses Paths within Hadoop
import org.apache.hadoop.conf.*; //Configures Map Reduce Jobs
import org.apache.hadoop.io.*; //Input/Output in Hadoop
//All these packages are present in hadoop-mapreduce-client-core.jar
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
//The packages above are a must for writing a MapReduce program

public class StartingLetterOccurence {

	public static void main(String[] args)throws Exception{
		
		Configuration conf=new Configuration(); //Define entire configuration of MapReduce
		Job job = Job.getInstance(conf,"StartingLetterOccurence"); //Define the job by passing configuration and name of mapreduce program
		
		//Set Jar, Mapper, Reducer
		job.setJarByClass(StartingLetterOccurence.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		//This is what we are outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		Path outputPath = new Path(args[1]);
		
		//Configuring the input/output path from the filesystem into the job.
		FileInputFormat.addInputPath(job, new Path(args[0])); 
		FileOutputFormat.setOutputPath(job,new Path(args[1]));
		//Equivalent to executing: hadoop jar wordcount.jar /input /output
		
		 	
		//deleting the output path automatically from hdfs so that we don't have to delete it explicitly
		outputPath.getFileSystem(conf).delete(outputPath,true);
		
		//exiting the job only if the flag value becomes false
		System.exit(job.waitForCompletion(true)?0:1);
		
	}
	
	//Mapper Class inherits Super Class Mapper
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>{
		//Mapper takes in 4 Arguments: KEYIN,VALUEIN,KEYOUT,VALUEOUT
		//LongWritable is the offset location of where the text is. In this case, the location of the start of each line	
	
		//Ex: This is Jian Jian
		
		public void map(LongWritable key, Text value, Context context) throws IOException,InterruptedException,StringIndexOutOfBoundsException{
			//Context is another class that allow you to write the output of the mapper class
			
			String line = value.toString(); //Take in each line and turn it into a string.
			StringTokenizer tokenizer = new StringTokenizer(line); //Extracts the word based on the spaces between them
			
			//Using the object, go through each word
			while(tokenizer.hasMoreTokens()){
				value.set(tokenizer.nextToken().substring(0,1));	
				context.write(value, new IntWritable(1)); //Write the key (word) and value (Assign 1 for occurence) 
			}
		}
		
		
	}

	//Reducer Class inherits Super Class Reducer
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,InterruptedException{  
		//Iterable is the list of values against a particular key			
		int sum=0;
		for (IntWritable x: values){ //Pass in values of IntWritable. Read every value inside Iterable.
			sum+=x.get(); //Add values. In this case, all of it is 1.
		}
		context.write(key, new IntWritable(sum)); //Write down the key (word) and value (Sum of occurences of the word)
		}		
	}
	
}
