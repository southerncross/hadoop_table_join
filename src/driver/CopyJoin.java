package driver;

import java.io.IOException;

import mapper.CopyMapper;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;

import data.Conf;
import reducer.CopyReducer;

/**
 * Driver of CopyJoin(or DuplicateJoin)
 * 
 * Implements JoinDriver interface
 * 
 * @author lishunyang
 */
public class CopyJoin implements JoinDriver {

	/**
	 * Unite test
	 * 
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
		if (!conf.loadConf(userDir + "/conf.properties")) {
			System.err.println("Failed in loading configuration file, exit");
			System.exit(-2);
		}

		new CopyJoin().join(args, conf);
	}

	public void join(String[] args, Conf conf) throws IOException {
		JobConf job = new JobConf(CopyJoin.class);
		job.setJobName("Copy Join");

		Path inLeft = new Path(args[0]);
		Path inRight = new Path(args[1]);
		Path out = new Path(args[2]);

		// determine which one is smaller and which one is larger
		FileSystem hdfs = inLeft.getFileSystem(new Configuration());
		FileStatus inLeftStatus = hdfs.getFileStatus(inLeft);
		FileStatus inRightStatus = hdfs.getFileStatus(inRight);
		Path inSmall, inLarge;
		if (inLeftStatus.getLen() < inRightStatus.getLen()) {
			inSmall = new Path(inLeft.toString());
			inLarge = new Path(inRight.toString());
		} else {
			inSmall = new Path(inRight.toString());
			inLarge = new Path(inLeft.toString());
		}

		DistributedCache.addCacheFile(inSmall.toUri(), job);

		FileInputFormat.addInputPath(job, inLarge);
		FileOutputFormat.setOutputPath(job, out);

		job.setMapperClass(CopyMapper.class);
		job.setReducerClass(CopyReducer.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);

		// configuration
		job.set("inputNameLeft", inLeft.toString());
		job.set("inputNameSmall", inSmall.toString());
		job.set("mapred.textoutputformat.separator", conf.separator);
		job.setBoolean("mapred.conf.eliminateDuplicate",
				conf.eliminateDuplicate);

		JobClient.runJob(job);
	}
}
