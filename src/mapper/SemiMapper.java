package mapper;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;

import data.Record;

/**
 * Mapper of SemiJoin
 * 
 * @author lishunyang
 */
public class SemiMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Record> {

	protected String inputNameLeft; // left-side table
	private BloomFilter bf = new BloomFilter();
	private String separator;

	// read bloomfilter into memory
	@Override
	public void configure(JobConf job) {
		inputNameLeft = job.get("inputNameLeft");
		separator = job.get("mapred.textoutputformat.separator");
		Path[] cacheFiles;

		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(job);
			if (null != cacheFiles && cacheFiles.length > 0) {
				FSDataInputStream in = FileSystem.getLocal(new Configuration())
						.open(cacheFiles[0]);
				try {
					bf.readFields(in);
				} finally {
					in.close();
				}
			}
		} catch (IOException e) {
			System.err.println("Exception reading DistributedCache: " + e);
		}
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Record> output, Reporter reporter)
			throws IOException {
		String splitName = ((FileSplit) reporter.getInputSplit()).getPath()
				.toString().substring(5); // remove the prefix "file:"
		String line = value.toString();
		int pos;
		String k, v;
		Record r;

		System.out.println(splitName + ":" + inputNameLeft);
		// left-side table: v k
		if (splitName.equals(inputNameLeft)) {
			pos = line.lastIndexOf(separator);
			k = line.substring(pos + 1);
			v = line.substring(0, pos);
			r = new Record();
			r.setValue(v);
			r.setFlag(0); // "0" means left
		}
		// right-side table: k v
		else {
			pos = line.indexOf(separator);
			k = line.substring(0, pos);
			v = line.substring(pos + 1);
			r = new Record();
			r.setValue(v);
			r.setFlag(1); // "1" means right
		}
		// semi-join
		if (bf.membershipTest(new Key(k.getBytes()))) {
			System.out.println(r.getFlag());
			output.collect(new Text(k), r);
		}
	}
}
