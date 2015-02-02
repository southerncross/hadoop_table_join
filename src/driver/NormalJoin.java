package driver;

import java.io.IOException;

import mapper.NormalMapper;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import data.Conf;
import data.Record;
import reducer.NormalReducer;

/**
 * Driver of NormalJoin(or BasicJoin)
 * 
 * Implements JoinDriver interface
 * 
 * @author lishunyang
 * 
 */
public class NormalJoin implements JoinDriver{

	/**
	 * Unite test
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		if (args.length != 3) {
			System.err
					.println("Usage: Join <left-side table path> <right-side table path> <output path>");
			System.exit(-1);
		}
		String userDir = System.getProperty("user.dir");
		Conf conf = new Conf();
		if (!conf.loadConf(userDir + "/conf.properties")) { // TODO
			System.err.println("Failed in loading configuration file, exit");
			System.exit(-2);
		}

		new NormalJoin().join(args, conf);
	}

	public void join(String[] args, Conf conf) throws IOException {
		JobConf job = new JobConf(NormalJoin.class);
		job.setJobName("Equal Join");

		Path inLeft = new Path(args[0]);
		Path inRight = new Path(args[1]);
		Path out = new Path(args[2]);

		FileInputFormat.addInputPath(job, inLeft);
		FileInputFormat.addInputPath(job, inRight);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(NormalMapper.class);
		job.setReducerClass(NormalReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapOutputValueClass(Record.class);

		// configuration
		job.set("inputNameLeft", inLeft.toString());
		job.set("mapred.textoutputformat.separator", conf.separator);
		job.setBoolean("mapred.conf.eliminateDuplicate", conf.eliminateDuplicate);

		JobClient.runJob(job);
	}

}
