package data;

import org.apache.hadoop.fs.Path;

/**
 * Used to describe join table
 * 
 * @author lishunyang
 * 
 */
public class Table {
	public Path path;
	public long size;
	public long numBlock;
	public long numTuple;
	public long sizeBlock;
	public long sizeTuple;
	public long distinctLeftKey;
	public long distinctRightKey;
	public boolean isSampling;

	public Table() {
		this.path = null;
		this.size = -1;
		this.numBlock = -1;
		this.numTuple = -1;
		this.sizeBlock = -1;
		this.sizeTuple = -1;
		this.distinctLeftKey = -1;
		this.distinctRightKey = -1;
		this.isSampling = false;
	}

	public Table(String path) {
		this.path = new Path(path);
		this.size = -1;
		this.numBlock = -1;
		this.numTuple = -1;
		this.sizeBlock = -1;
		this.sizeTuple = -1;
		this.distinctLeftKey = -1;
		this.distinctRightKey = -1;
		isSampling = false;
	}

	public Table(Table t) {
		if (null != t.path)
			this.path = new Path(t.path.toString());
		this.size = t.size;
		this.numBlock = t.numBlock;
		this.numTuple = t.numTuple;
		this.sizeBlock = t.sizeBlock;
		this.sizeTuple = t.sizeTuple;
		this.distinctLeftKey = t.distinctLeftKey;
		this.distinctRightKey = t.distinctRightKey;
		this.isSampling = t.isSampling;
	}

	/**
	 * Print self, used for debugging
	 */
	public void printSelf() {
		System.out.println("path:\t" + path.toString());
		System.out.println("size:\t" + size);
		System.out.println("numBlock:\t" + numBlock);
		System.out.println("numTuple:\t" + numTuple);
		System.out.println("sizeBlock:\t" + sizeBlock);
		System.out.println("sizeTuple:\t" + sizeTuple);
		System.out.println("distinctLeftKey:\t" + distinctLeftKey);
		System.out.println("distinctRightKey:\t" + distinctRightKey);
	}

	public String info() {
		return new String(size + " " + numBlock);
	}
}
