package reducer;

import java.io.IOException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import data.Record;

/**
 * Reducer of NormalJoin(or BasicJoin)
 * 
 * @author lishunyang
 * 
 */
public class NormalReducer extends MapReduceBase implements
		Reducer<Text, Record, Text, Text> {

	private String separator;
	private boolean eliminateDuplicate;

	@Override
	public void configure(JobConf job) {
		separator = job.get("mapred.textoutputformat.separator");
		eliminateDuplicate = job.getBoolean("mapred.conf.eliminateDuplicate",
				true);
	}

	@Override
	public void reduce(Text key, Iterator<Record> values,
			OutputCollector<Text, Text> output, Reporter reporter)
			throws IOException {
		Record r;
		List<String> listLeft;
		List<String> listRight;
		Set<String> setLeft;
		Set<String> setRight;

		if (!eliminateDuplicate) { // do not eliminate duplicate
			listLeft = new LinkedList<String>();
			listRight = new LinkedList<String>();
			while (values.hasNext()) {
				r = values.next();
				// left-side table
				if (r.getFlag() == 0) {
					listLeft.add(r.getValue());
				}
				// right-side table
				else
					listRight.add(r.getValue());
			}
			for (String vl : listLeft) {
				for (String vr : listRight) {
					output.collect(new Text(vl), new Text(key + separator + vr));
				}
			}
		} else { // eliminate duplicate
			setLeft = new HashSet<String>();
			setRight = new HashSet<String>();
			while (values.hasNext()) {
				r = values.next();
				// left-side table
				if (r.getFlag() == 0) {
					setLeft.add(r.getValue());
				}
				// right-side table
				else
					setRight.add(r.getValue());
			}
			for (String vl : setLeft) {
				for (String vr : setRight) {
					output.collect(new Text(vl), new Text(key + separator + vr));
				}
			}
		}
	}
}
