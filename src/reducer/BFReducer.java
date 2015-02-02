package reducer;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.bloom.BloomFilter;
import org.apache.hadoop.util.hash.Hash;

/**
 * Reducer of SemiJoin, used for generating bloom filter
 * 
 * @author lishunyang
 */
public class BFReducer extends MapReduceBase implements
		Reducer<Text, BloomFilter, Text, Text> {

	private BloomFilter bf;
	private JobConf job = null;
	private int sizeBloomfilter;

	@Override
	public void configure(JobConf job) {
		this.job = job;
		sizeBloomfilter = job.getInt("mapred.conf.sizeBloomfilter", 1024);
		bf = new BloomFilter(sizeBloomfilter, 6, Hash.JENKINS_HASH);
	}

	@Override
	public void reduce(Text key, Iterator<BloomFilter> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		while (values.hasNext())
			bf.or(values.next());
	}

	@Override
	public void close() throws IOException {
		Path file = new Path(FileOutputFormat.getOutputPath(job)
				+ "/bloomfilter");
		FSDataOutputStream out = file.getFileSystem(job).create(file);
		bf.write(out);
		out.close();
	}

}
