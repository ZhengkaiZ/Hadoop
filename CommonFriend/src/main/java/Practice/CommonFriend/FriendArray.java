/**
 * 
 */
package Practice.CommonFriend;

import org.apache.hadoop.io.Writable;


import org.apache.hadoop.io.ArrayWritable;

/**
 * @author Zhengkai Zhang
 */
public class FriendArray extends ArrayWritable {
	
	public FriendArray() {
		super(Friend.class);
	}

	public FriendArray(Class<? extends Writable> valueClass) {
		super(valueClass);
	}
	
	public FriendArray(Class<? extends Writable> valueClass, Writable[] values) {
		super(valueClass, values);
	}

	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		String[] strings = this.toStrings();
		for (String s : strings) {
			sb.append(s);
		}
		
		return sb.toString();
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof FriendArray)) {
			return false;
		}

		FriendArray temp = (FriendArray) o;
		Friend[] f1 = (Friend[]) this.get();
		Friend[] f2 = (Friend[]) temp.get();
		if (f1.length != f2.length) {
			return false;
		}

		int length = f1.length;
		for (int i = 0; i < length; i++) {
			if (!f1[i].equals(f2[i])) {
				return false;
			}
		}

		return true;
	}
	
}
