/**
 * 
 */
package Practice.CommonFriend;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;
import org.apache.hadoop.io.Text;

/**
 * @author Zhengkai Zhang
 *
 */

	
public class CommonFriendReducer extends Reducer<FriendPair, FriendArray, FriendPair, FriendArray> {
	public enum Volume {
		High_Volume
	}


	@Override
	public void reduce(FriendPair key, Iterable<FriendArray> values, Context context) throws IOException, InterruptedException {
		Logger log = Logger.getLogger(CommonFriendReducer.class);

		List<Friend[]> friendArrayList = new ArrayList<Friend[]>();
		for (FriendArray friendArray : values) {
			Friend[] tempArray = Arrays.copyOf(friendArray.get(), friendArray.get().length, Friend[].class);
			friendArrayList.add(tempArray);
		}


		if (friendArrayList.size() != 2) {
			log.debug("Invalid Data");
			return;
		}
		
		List<Friend> friendList = new ArrayList<Friend>();
		Friend[] friendArray1 = friendArrayList.get(0);
		Friend[] friendArray2 = friendArrayList.get(1);
		for (Friend friend1 : friendArray1) {
			for (Friend friend2 : friendArray2) {
				if (friend1.equals(friend2)) {
					friendList.add(friend1);
				}
			}
		}
		if (true) {
			context.getCounter(Volume.High_Volume).increment(1);
		}
		Friend[] tempArray2 = Arrays.copyOf(friendList.toArray(), friendList.size(), Friend[].class);
		FriendArray result = new FriendArray(Friend.class, tempArray2);
		context.write(key, result);
	}
}
