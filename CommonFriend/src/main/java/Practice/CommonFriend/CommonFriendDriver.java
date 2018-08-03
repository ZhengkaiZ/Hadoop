package Practice.CommonFriend;
/**
 * Common Friend MapReduce Driver
 * @author Zhengkai Zhang
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

public class CommonFriendDriver extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		
		if (args.length != 2) {
			System.err.println("Usage: fberature <input path> <output path>");
			System.exit(-1);
		}

		//Job Setup
		Job commonFriend = Job.getInstance(getConf(), "facebook-friends");
		
		commonFriend.setJarByClass(CommonFriendDriver.class);
		
		
		//File Input and Output format
		FileInputFormat.addInputPath(commonFriend, new Path(args[0]));
		FileOutputFormat.setOutputPath(commonFriend, new Path(args[1]));
		
		commonFriend.setInputFormatClass(TextInputFormat.class);
		commonFriend.setOutputFormatClass(SequenceFileOutputFormat.class);

		//Mapper-Reducer-Combiner specifications
		commonFriend.setMapperClass(CommonFriendMapper.class);
		commonFriend.setReducerClass(CommonFriendReducer.class);
		
		commonFriend.setMapOutputKeyClass(FriendPair.class);
		commonFriend.setMapOutputValueClass(FriendArray.class);

		//Output key and value
		commonFriend.setOutputKeyClass(FriendPair.class);
		commonFriend.setOutputValueClass(FriendArray.class);
		
		//Submit job
		return commonFriend.waitForCompletion(true) ? 0 : 1;
		
	}

	public static void main(String[] args) throws Exception {

		int exitCode = ToolRunner.run(new CommonFriendDriver(), args);
		System.exit(exitCode);
	}
}
