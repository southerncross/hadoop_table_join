package mapper;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper of CopyJoin(or DuplicateJoin)
 * 
 * @author lishunyang
 */
public class CopyMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Text> {

	private String inputNameLeft;
	private String inputNameSmall;
	private Hashtable<String, Set<String>> inSetSmall = new Hashtable<String, Set<String>>(); // used when eliminate duplicate
	private Hashtable<String, List<String>> inListSmall = new Hashtable<String, List<String>>(); // used when don't eliminate duplicate
	private String separator;
	private boolean eliminateDuplicate;

	// read smaller table into memory
	@Override
	public void configure(JobConf job) {
		inputNameLeft = job.get("inputNameLeft");
		inputNameSmall = job.get("inputNameSmall");
		separator = job.get("mapred.textoutputformat.separator");
		eliminateDuplicate = job.getBoolean("mapred.conf.eliminateDuplicate",
				true);
		Path[] cacheFiles;

		try {
			cacheFiles = DistributedCache.getLocalCacheFiles(job);
			if (null != cacheFiles && cacheFiles.length > 0) {
				String line;
				String k, v;
				int pos;
				BufferedReader joinReader = new BufferedReader(new FileReader(
						cacheFiles[0].toString())); // read cache file
				try {
					// left-side table is smaller, v k
					if (inputNameLeft.equalsIgnoreCase(inputNameSmall)) {
						while ((line = joinReader.readLine()) != null) {
							pos = line.lastIndexOf(separator);
							k = line.substring(pos + 1);
							v = line.substring(0, pos);
							if (!eliminateDuplicate) { // do not eliminate
														// duplicate
								if (inListSmall.containsKey(k))
									inListSmall.get(k).add(v);
								else {
									List<String> l = new LinkedList<String>();
									l.add(v);
									inListSmall.put(k, l);
								}
							} else { // eliminate duplicate
								if (inSetSmall.containsKey(k))
									inSetSmall.get(k).add(v);
								else {
									Set<String> s = new HashSet<String>();
									s.add(v);
									inSetSmall.put(k, s);
								}
							}
						}
						// right-side table is smaller, k v
					} else {
						while ((line = joinReader.readLine()) != null) {
							pos = line.indexOf(separator);
							k = line.substring(0, pos);
							v = line.substring(pos + 1);
							if (!eliminateDuplicate) { // do not eliminate
														// duplicate
								if (inListSmall.containsKey(k))
									inListSmall.get(k).add(v);
								else {
									List<String> l = new LinkedList<String>();
									l.add(v);
									inListSmall.put(k, l);
								}
							} else { // eliminate duplicate
								if (inSetSmall.containsKey(k))
									inSetSmall.get(k).add(v);
								else {
									Set<String> s = new HashSet<String>();
									s.add(v);
									inSetSmall.put(k, s);
								}
							}
						}
					}
				} finally {
					joinReader.close();
				}
			}
		} catch (IOException e) {
			System.err.println("Exception reading DistributedCache: " + e);
		}
	}

	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		String line = value.toString();
		String k, v;
		int pos;
		List<String> listSmall;
		Set<String> setSmall;

		// right-side table is large, k v
		if (inputNameLeft.equalsIgnoreCase(inputNameSmall)) {
			pos = line.indexOf(separator);
			k = line.substring(0, pos);
			v = line.substring(pos + 1);
			if (!eliminateDuplicate) { // do not eliminate duplicate
				listSmall = inListSmall.get(k);
				if (listSmall != null) {
					for (String vl : listSmall) {
						output.collect(new Text(vl),
								new Text(k + separator + v));
					}
				}
			}
			else { // eliminate duplicate
				setSmall = inSetSmall.get(k);
				if (setSmall != null) {
					for (String vl : setSmall) {
						output.collect(new Text(vl),
								new Text(k + separator + v));
					}
				}
			}
		}
		// left-side table is large, v k
		else {
			pos = line.lastIndexOf(separator);
			k = line.substring(pos + 1);
			v = line.substring(0, pos);
			if (!eliminateDuplicate) { // do not eliminate duplicate
				listSmall = inListSmall.get(k);
				if (listSmall != null) {
					for (String vr : listSmall) {
						output.collect(new Text(v),
								new Text(k + separator + vr));
					}
				}
			}
			else { // eliminate duplicate
				setSmall = inSetSmall.get(k);
				if (setSmall != null) {
					for (String vr : setSmall) {
						output.collect(new Text(v),
								new Text(k + separator + vr));
					}
				}
			}
		}
	}
}
