package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.bloom.Key;
import org.apache.hadoop.util.hash.Hash;

/**
 * Mapper of SemiJoin, which is used for generating bloom filter
 * 
 * @author lishunyang
 */
public class BFMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, BloomFilter> {

	private BloomFilter bf;
	OutputCollector<Text, BloomFilter> oc;
	private String inputNameLeft;
	private String inputNameSmall;
	private String separator;
	private int sizeBloomfilter;

	@Override
	public void configure(JobConf job) {
		inputNameLeft = job.get("inputNameLeft");
		inputNameSmall = job.get("inputNameSmall");
		separator = job.get("mapred.textoutputformat.separator");
		sizeBloomfilter = job.getInt("mapred.conf.sizeBloomfilter", 1024);
		bf = new BloomFilter(sizeBloomfilter, 6, Hash.JENKINS_HASH);
		oc = null;
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, BloomFilter> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		int pos;
		String k;

		if (null == oc)
			oc = output;

		// left-side table is smaller, which will be used to generate
		// bloom filter: v k
		if (inputNameLeft.equalsIgnoreCase(inputNameSmall)) {
			pos = line.lastIndexOf(separator);
			k = line.substring(pos + 1);
		}
		// right-side table is smaller, which will be used to generate
		// bloom filter: k v
		else {
			pos = line.indexOf(separator);
			k = line.substring(0, pos);
		}
		bf.add(new Key(k.getBytes()));
	}

	@Override
	public void close() throws IOException {
		oc.collect(new Text("BloomFilter"), bf);
	}
}
