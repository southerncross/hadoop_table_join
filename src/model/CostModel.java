package model;

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import data.Conf;
import data.Method;
import data.Table;

/**
 * Sampling, estimate join cost and estimate join result
 * 
 * @author lishunyang
 * 
 */
public class CostModel {

	private Conf conf;

	public CostModel(Conf conf) {
		this.conf = conf;
	}

	/**
	 * Sampling given table for basic information, such as size, number of
	 * tuples, number of blocks...
	 * 
	 * @note We assume that the size of a block is 64K
	 * @param table
	 * @throws Exception
	 */
	public void sampling(Table table) throws Exception {
		if (table.isSampling)
			return;
		table.isSampling = true;

		if (null == table.path)
			throw new Exception("Uncompleted table error: no path");
		Path p = table.path;
		FileSystem hdfs = p.getFileSystem(new Configuration());
		FileStatus fileStatus = hdfs.getFileStatus(p);

		// table.sizeBlock = fileStatus.getBlockSize(); // this is too big
		table.sizeBlock = 1 << 16; // 64k
		table.size = fileStatus.getLen(); // size of table
		table.numBlock = (table.size + table.sizeBlock - 1) / table.sizeBlock;
		long numBlockSampling = (long) (table.numBlock * ((double) conf.samplingPercent / 100));
		if (0 == numBlockSampling)
			numBlockSampling = 1;

		int bsize = (int) table.sizeBlock;
		byte[] buffer = new byte[bsize];
		Set<String> distinctLeftKey = new HashSet<String>();
		Set<String> distinctRightKey = new HashSet<String>();
		long numTupleSampling = 0;

		FSDataInputStream in = null;
		try {
			in = hdfs.open(p);
			for (int i = 0; i < numBlockSampling; i++) {
				long pos = i * table.sizeBlock;
				in.read(pos, buffer, 0, bsize);
				String content = new String(buffer, "UTF-8");
				String[] lines = content.split("\n");
				for (int j = 0; j < lines.length - 1; j++) {
					String[] records = lines[j].split(conf.separator);
					numTupleSampling++;
					distinctLeftKey.add(records[0]);
					distinctRightKey.add(records[records.length - 1]);
				}
			}
		} finally {
			IOUtils.closeStream(in);
		}

		/* number of distinct left-key */
		table.distinctLeftKey = distinctLeftKey.size();
		/* number of distinct right-key */
		table.distinctRightKey = distinctRightKey.size();
		if (table.size < table.sizeBlock)
			table.numTuple = numTupleSampling; // number of tuple
		else
			table.numTuple = (long) ((double) numTupleSampling * ((double) table.numBlock / (double) numBlockSampling)); // number
																															// of
																															// tuple
		if (table.numTuple == 0)
			table.sizeTuple = 0;
		else
			table.sizeTuple = table.size / table.numTuple; // size of tuple
	}

	public Table estimateJoinTable(Table left, Table right) {
		Table result = new Table();
		result.sizeTuple = left.sizeTuple + right.sizeTuple;
		result.sizeBlock = left.sizeBlock;
		result.distinctLeftKey = left.distinctLeftKey;
		result.distinctRightKey = right.distinctRightKey;

		if (left.distinctRightKey < right.distinctLeftKey)
			result.numTuple = left.numTuple * right.numTuple
					/ right.distinctLeftKey;
		else
			result.numTuple = left.numTuple * right.numTuple
					/ left.distinctRightKey;
		result.numBlock = result.numTuple * result.sizeTuple / result.sizeBlock;
		result.numBlock = result.numBlock == 0 ? 1 : result.numBlock;

		return result;
	}

	/**
	 * Giving left table, right table and result table, estimate join cost
	 * 
	 * @param left
	 * @param right
	 * @param result
	 * @param method
	 * @return cost
	 */
	public long estimateCost(Table left, Table right, Table result,
			Method method) {
		long t = -1;
		long R, S, T;

		T = result.numBlock;
		if (left.numBlock < right.numBlock) {
			R = right.numBlock;
			S = left.numBlock;
		} else {
			R = left.numBlock;
			S = right.numBlock;
		}

		switch (method) {
		case NORMALJOIN:
			t = conf.launchDelay + (3 * R + 3 * S + T) * conf.IODelay
					+ (2 * R + 2 * S) * conf.transDelay;
			break;
		case COPYJOIN:
			t = conf.launchDelay + (R + 3 * T) * conf.IODelay
					+ (conf.numMapper * S + R + T) * conf.transDelay;
			break;
		case SEMIJOIN:
			t = 2 * conf.launchDelay + (R + 2 * S + 3 * T) * conf.IODelay
					+ (R + 2 * S + T) * conf.transDelay;
			break;
		}

		return t;
	}

	/**
	 * Select the best join method according to size of memory and tables
	 * 
	 * @param left
	 * @param right
	 * @return
	 */
	public Method selectMethod(Table left, Table right) {
		long sizeBloomFilter;

		if (left.size < conf.maxSizeMemory || right.size < conf.maxSizeMemory)
			return Method.COPYJOIN;
		sizeBloomFilter = left.size < right.size ? left.size / conf.bfRate
				: right.size / conf.bfRate;
		if (sizeBloomFilter < conf.maxSizeMemory)
			return Method.SEMIJOIN;
		return Method.NORMALJOIN;
	}

	/**
	 * Unit test
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		String userDir = System.getProperty("user.dir");
		Table table = new Table(args[0]);
		Conf conf = new Conf();
		if (!conf.loadConf(userDir + "/conf.properties")) {
			System.err.println("Failed in loading configuration file, exit");
			System.exit(-2);
		}
		new CostModel(conf).sampling(table);
		table.printSelf();
	}
}
