package mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import data.Record;

/**
 * Mapper of NormalJoin(or BasicJoin)
 * 
 * @author lishunyang
 */
public class NormalMapper extends MapReduceBase implements
		Mapper<LongWritable, Text, Text, Record> {

	protected String inputNameLeft; // left-side table
	private String separator;

	// determine which one is on the left and which one is on the right
	@Override
	public void configure(JobConf job) {
		inputNameLeft = job.get("inputNameLeft");
		separator = job.get("mapred.textoutputformat.separator");
	}

	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Record> output, Reporter reporter)
			throws IOException {
		String splitName = ((FileSplit) reporter.getInputSplit()).getPath().toString();
		String line = value.toString();
		int pos;
		String k, v;

		if (splitName.equals(inputNameLeft)) { // left-side table: v k
			pos = line.lastIndexOf(separator);
			k = line.substring(pos + 1);
			v = line.substring(0, pos);
			Record r = new Record();
			r.setValue(v);
			r.setFlag(0); // "0" means left
			output.collect(new Text(k), r);
		} else { // right-side table: k v
			pos = line.indexOf(separator);
			k = line.substring(0, pos);
			v = line.substring(pos + 1);
			Record r = new Record();
			r.setValue(v);
			r.setFlag(1); // "1" means right
			output.collect(new Text(k), r);
		}
	}
}
