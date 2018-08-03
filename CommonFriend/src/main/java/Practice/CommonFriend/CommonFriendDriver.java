package Practice.CommonFriend;
/**
 * Common Friend MapReduce Driver
 * @author Zhengkai Zhang
 */

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CommonFriendDriver {

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		if (args.length != 2) {
			throw new IllegalArgumentException();
		}
		Configuration conf = new Configuration();
		Job commonFriend = new Job(conf, "CommonFriend");
		commonFriend.setJarByClass(CommonFriendDriver.class);
		
		FileInputFormat.addInputPath(commonFriend, new Path(args[0]));
		FileOutputFormat.setOutputPath(commonFriend, new Path(args[1]));
		
		commonFriend.setInputFormatClass(TextInputFormat.class);
		commonFriend.setOutputFormatClass(TextOutputFormat.class);
		
		commonFriend.setMapperClass(CommonFriendMapper.class);
		commonFriend.setReducerClass(CommonFriendReduce.class);
		
		commonFriend.setMapOutputKeyClass(FriendPair.class);
		commonFriend.setMapOutputValueClass(FriendArray.class);
	}
}
