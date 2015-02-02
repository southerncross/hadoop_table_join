package reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

/**
 * Reducer of CopyJoin(or DuplicateJoin)
 * 
 * @author lishunyang
 * 
 */
public class CopyReducer extends MapReduceBase implements
		Reducer<Text, Text, Text, Text> {

	private boolean eliminateDuplicate;

	@Override
	public void configure(JobConf job) {
		eliminateDuplicate = job.getBoolean("mapred.conf.eliminateDuplicate",
				true);
	}

	// write output file back to HDFS
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		Set<String> valueSet = new HashSet<String>();
		String value;

		while (values.hasNext()) {
			value = values.next().toString();
			if (eliminateDuplicate) { // eliminate duplicate
				if (!valueSet.contains(value)) {
					valueSet.add(value);
					output.collect(new Text(key), new Text(value));
				}
			}
			else { // don't eliminate duplicate
				output.collect(new Text(key), new Text(value));
			}
		}
	}
}
