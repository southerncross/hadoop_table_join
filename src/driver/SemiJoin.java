package driver;

import java.io.IOException;

import mapper.BFMapper;
import mapper.SemiMapper;

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
import org.apache.hadoop.util.bloom.BloomFilter;

import data.Conf;
import data.Record;
import reducer.BFReducer;
import reducer.NormalReducer;

/**
 * Driver of SemiJoin
 * 
 * It's still an eqaul join, but using semijoin technology
 * 
 * @author lishunyang
 */
public class SemiJoin implements JoinDriver {

	/**
	 * Unite test
	 * @param args
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws ClassNotFoundException,
			IOException, InterruptedException {
		// unit test
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

		new SemiJoin().join(args, conf);
		// delete temporaray direction
		FileSystem hdfs = new Path(args[0]).getFileSystem(new Configuration());
		hdfs.delete(conf.tmpPath, true);
	}

	public void join(String[] args, Conf conf) throws IOException {
		/* direction of bloomfilter */
		Path bfPath = new Path(conf.tmpPath.toString() + "/bloomfilter");
		Path inLeft = new Path(args[0]);
		Path inRight = new Path(args[1]);
		Path out = new Path(args[2]);
		int bfSize = 0;

		// determine which one is smaller and which one is larger
		FileSystem hdfs = inLeft.getFileSystem(new Configuration());
		FileStatus inLeftStatus = hdfs.getFileStatus(inLeft);
		FileStatus inRightStatus = hdfs.getFileStatus(inRight);
		Path inSmall;
		if (inLeftStatus.getLen() < inRightStatus.getLen()) {
			inSmall = new Path(inLeft.toString());
			bfSize = (int) (inLeftStatus.getLen() / conf.bfRate);
		} else {
			inSmall = new Path(inRight.toString());
			bfSize = (int) (inRightStatus.getLen() / conf.bfRate);
		}
		bfSize = bfSize < conf.minBfSize ? conf.minBfSize : bfSize;
		bfSize = bfSize > conf.maxBfSize ? conf.maxBfSize : bfSize;

		// generate bloomfilter for smaller table
		JobConf bfJob = new JobConf(SemiJoin.class);
		bfJob.setJobName("BloomFilter Join");
		FileInputFormat.addInputPath(bfJob, inSmall);
		FileOutputFormat.setOutputPath(bfJob, bfPath);
		bfJob.setMapperClass(BFMapper.class);
		bfJob.setReducerClass(BFReducer.class);
		bfJob.setOutputKeyClass(Text.class);
		bfJob.setOutputValueClass(Text.class);
		bfJob.setMapOutputValueClass(BloomFilter.class);
		bfJob.set("inputNameLeft", inLeft.toString());
		bfJob.set("inputNameSmall", inSmall.toString());
		bfJob.set("mapred.textoutputformat.separator", conf.separator);
		bfJob.setInt("mapred.conf.sizeBloomfilter", bfSize);
		JobClient.runJob(bfJob);

		// join
		JobConf semiJob = new JobConf(SemiJoin.class);
		semiJob.setJobName("Semi Join");
		DistributedCache.addCacheFile(
				(new Path(bfPath + "/bloomfilter")).toUri(), semiJob);
		FileInputFormat.addInputPath(semiJob, inLeft);
		FileInputFormat.addInputPath(semiJob, inRight);
		FileOutputFormat.setOutputPath(semiJob, out);
		semiJob.setMapperClass(SemiMapper.class);
		semiJob.setReducerClass(NormalReducer.class);
		semiJob.setOutputKeyClass(Text.class);
		semiJob.setOutputValueClass(Text.class);
		semiJob.setMapOutputValueClass(Record.class);
		semiJob.set("inputNameLeft", inLeft.toString());
		semiJob.set("mapred.textoutputformat.separator", conf.separator);
		semiJob.setBoolean("mapred.conf.eliminateDuplicate",
				conf.eliminateDuplicate);
		JobClient.runJob(semiJob);
		
		// delete bloom filter
		hdfs.delete(bfPath, true);
	}

}
