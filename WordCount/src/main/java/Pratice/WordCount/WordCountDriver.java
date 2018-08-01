/**
 * 
 */
package Pratice.WordCount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


/**
 * @author Zhengkai Zhang
 *
 */
public class WordCountDriver {
	/**
	 * @param args
	 * @throws Exception 
	 */
	public static void main(String[] args) throws Exception {
		// TODO Auto-generated method stub

		Configuration conf = new Configuration();
		// Start a job by set job name and setJarByClass
		Job wordCount = new Job(conf, "Word_Count");
		wordCount.setJarByClass(Pratice.WordCount.WordCountDriver.class);

		//Add input and output path from args
		FileInputFormat.addInputPath(wordCount, new Path(args[0]));
		FileOutputFormat.setOutputPath(wordCount, new Path(args[1]));

		//set input output format
		wordCount.setInputFormatClass(TextInputFormat.class);
		wordCount.setOutputFormatClass(TextOutputFormat.class);

		//set mapper and reducer class
		wordCount.setMapperClass(WordCountMapper.class);
		wordCount.setReducerClass(WordCountReduce.class);

		//set Map output Key and value class
		wordCount.setMapOutputKeyClass(Text.class);
		wordCount.setMapOutputValueClass(IntWritable.class);
		
		wordCount.waitForCompletion(true);

	}
}
