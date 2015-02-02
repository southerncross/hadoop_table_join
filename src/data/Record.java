package data;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/**
 * Tagged key-value Pair, used in join process
 * 
 * @author lishunyang
 */
public class Record implements Writable {

	private String value;
	private int flag = 0; // "0" means left-side, "1" means right-side

	public Record() {
	}

	public Record(Record r) {
		this.value = r.value;
		this.flag = r.flag;
	}
	
	public void setValue(String value) {
		this.value = value;
	}

	public String getValue() {
		return value;
	}
	
	public int getFlag() {
		return flag;
	}

	public void setFlag(int flag) {
		this.flag = flag;
	}

	@Override
	public void readFields(DataInput input) throws IOException {
		this.value = input.readUTF();
		this.flag = input.readInt();
	}

	@Override
	public void write(DataOutput output) throws IOException {
		output.writeUTF(this.value);
		output.writeInt(this.flag);
	}
	
	@Override
	public boolean equals(Object o) {
		Record r = (Record) o;
		if (this.value.equalsIgnoreCase(r.value) && this.flag == r.flag)
			return true;
		else
			return false;
	}

}
