
package Practice.CommonFriend;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.log4j.Logger;
import org.apache.taglibs.standard.lang.jstl.parser.ParseException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

/**
 * @author Zhengkai Zhang
 * Credit to Hadoop in real world 
 * Learn Hadoop from Online Course from Hadoop in real world.
 */

public class CommonFriendMapper extends Mapper<LongWritable, Text, FriendPair, FriendArray> {
	@Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		Logger log = Logger.getLogger(CommonFriendMapper.class);
		
		String[] input = value.toString().split("\t");
		Friend thisPerson = getPerson(input[0]);
		List<Friend> friendList = getFriendArray(input[1]);
		Friend[] temp = (Friend[]) friendList.toArray();
		FriendArray friends = new FriendArray(Friend.class, temp);

		for (Friend friend : friendList) {
			FriendPair fp = new FriendPair(thisPerson, friend);
			context.write(fp, friends);
			log.info(fp + "  " + friends);
		}
	}
	/**
	 * Get this person
	 * @param input First Person
	 * @return the Wrap Friend Class
	 */

	private Friend getPerson(String input) {
		JSONParser parser = new JSONParser();
		Friend result = null;
	
		JSONObject jsonObj;
		try {
			jsonObj = (JSONObject) parser.parse(input);
	
			IntWritable id = new IntWritable((Integer) jsonObj.get("id"));
			Text name = new Text((String) jsonObj.get("name"));
			Text hometown = new Text((String) jsonObj.get("hometown"));

			result = new Friend(id, name, hometown);
		} catch (org.json.simple.parser.ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return result;
	}
	/**
	 * Get Friend List.
	 * @param input String.
	 * @return list Friend.
	 */
	
	private List<Friend> getFriendArray(String input) {
		JSONParser parser = new JSONParser();
		List<Friend> result = new ArrayList<Friend>();

		try {
			JSONArray jsonArray = (JSONArray) parser.parse(input);
	
			for (Object obj : jsonArray) {

				JSONObject jsonObj = (JSONObject) obj;
				IntWritable id = new IntWritable((Integer) jsonObj.get("id"));
				Text name = new Text((String) jsonObj.get("name"));
				Text hometown = new Text((String) jsonObj.get("hometown"));
				result.add(new Friend(id, name, hometown));
			}

		} catch (org.json.simple.parser.ParseException e) {
				e.printStackTrace();
		}

		return result;
	}
}
