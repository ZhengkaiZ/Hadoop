package Practice.CommonFriend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/**
 * 
 * @author Zhengkai Zhang
 */

public class FriendPair implements WritableComparable {
	Friend first;
	Friend second;
	public FriendPair() {
		first = new Friend();
		second = new Friend();
	}

	public FriendPair(Friend f1, Friend f2) {
		first = f1;
		second = f2;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		first.readFields(in);
		second.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		first.write(out);
		second.write(out);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if (!(o instanceof FriendPair)) {
			return -1;
		}
		
		FriendPair temp = (FriendPair) o;
		if (first.idGetter().equals(temp.first.idGetter()) || 
				second.idGetter().equals(second.idGetter())) {
			return 0;
		}
		
		if (first.idGetter().equals(temp.second.idGetter()) || 
				second.idGetter().equals(first.idGetter())) {
			return 0;
		}

		return first.idGetter().compareTo(temp.first.idGetter());
	}
	
	@Override
	public String toString() {
		return first.toString() + " " + second.toString();
	}
}
