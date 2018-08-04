package Practice.CommonFriend;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Friend WritableComparable 
 * @author Zhengkai Zhang
 */
public class Friend implements WritableComparable {
	private IntWritable id;
	private Text name;
	private Text hometown;
	
	public Friend() {
		id = new IntWritable();
		name = new Text();
		hometown = new Text();
	}
	
	public Friend(IntWritable id, Text name, Text hometown) {
		this.id = id;
		this.name = name;
		this.hometown = hometown;
	}
	
	public void idSetter(IntWritable id) {
		this.id = id;
	}
	
	public IntWritable idGetter() {
		return id;
	}
	
	public void nameSetter(Text name) {
		this.name = name;
	}
	
	public Text nameGetter() {
		return name;
	}
	
	public void hometownSetter(Text hometown) {
		this.hometown = hometown;
	}
	
	public Text hometownGetter() {
		return hometown;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		// TODO Auto-generated method stub
		id.readFields(in);
		name.readFields(in);
		hometown.readFields(in);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		// TODO Auto-generated method stub
		id.write(out);
		name.write(out);
		hometown.write(out);
	}

	@Override
	public int compareTo(Object o) {
		// TODO Auto-generated method stub
		if (o == null || !(o instanceof Friend)) {
			throw new IllegalArgumentException();
		}
		Friend temp = (Friend) o;
		return idGetter().compareTo(temp.idGetter());
	}
	@Override
	public boolean equals(Object o) {
		if (o == null || ! (o instanceof Friend)) {
			return false;
		}

		Friend temp = (Friend) o;
		return idGetter().equals(temp.idGetter());
	}
	@Override
	public String toString() {
		StringBuilder sb = new StringBuilder();
		sb.append("{id: " + id.toString() + " ");
		sb.append("name: " + name.toString() + " ");
		sb.append("hometown: " + hometown.toString() + "}");
		return sb.toString();
	}
}
