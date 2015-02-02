package data;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

/**
 * Load configuration file and is used during join process
 * 
 * @author lishunyang
 * 
 */
public class Conf {
	// resource limitation
	public long maxSizeMemory;
	public int numMapper;

	// temporary direction
	public Path tmpPath;

	// kinds of delay
	public long launchDelay;
	public long IODelay;
	public long transDelay;

	// log
	public boolean writeLog;

	// optimal engine type
	public String typeEngine;

	// separator in tables
	public String separator;

	// whether eliminate duplicate
	public boolean eliminateDuplicate;

	// sampling percent
	public int samplingPercent;

	// bloomfilter size rate
	public int bfRate = 1000;

	// minimum size of bloomfilter
	public int minBfSize = 1024;
	// maximum size of bloomfilter
	public int maxBfSize = 65536;
	
	// those configurations are used to generate testing data 
	public int minSizeKB = 1;
	public int maxSizeKB = 100;
	public int minDuplicate = 1;
	public int maxDuplicate = 2;
	public int minCoincide = 1;
	public int maxCoincide = 10;  

	/**
	 * Load configuration file
	 * 
	 * @param confPath
	 * @return
	 * @throws Exception 
	 */
	public boolean loadConf(String confPath) {
		InputStream in = null;
		Properties conf;
		boolean res = false;

		try {
			in = new BufferedInputStream(new FileInputStream(confPath));
			conf = new Properties();
			conf.load(in);

			maxSizeMemory = Long.valueOf(conf.getProperty("size_buffer",
					"65535"));
			numMapper = Integer.valueOf(conf.getProperty("num_mapper", "1"));
			tmpPath = new Path(conf.getProperty("tmp_dir"));
			launchDelay = Long.valueOf(conf.getProperty("launch_delay", "500"));
			IODelay = Long.valueOf(conf.getProperty("IO_delay", "10"));
			transDelay = Long.valueOf(conf.getProperty("trans_delay", "1"));
			writeLog = Boolean.valueOf(conf.getProperty("write_log", "false"));
			typeEngine = conf.getProperty("type_engine", "Greedy");
			separator = conf.getProperty("separator", "\t");
			eliminateDuplicate = Boolean.valueOf(conf.getProperty(
					"eliminate_duplicate", "true"));
			samplingPercent = Integer.valueOf(conf.getProperty(
					"sampling_percent", "10"));
			minSizeKB = Integer.valueOf(conf.getProperty("min_size", "1"));
			maxSizeKB = Integer.valueOf(conf.getProperty("max_size", "100"));
			minDuplicate = Integer.valueOf(conf.getProperty("min_duplicate", "1"));
			maxDuplicate = Integer.valueOf(conf.getProperty("max_duplicate", "2"));
			minCoincide = Integer.valueOf(conf.getProperty("min_coincide", "1"));
			maxCoincide = Integer.valueOf(conf.getProperty("max_coincide", "10"));
			if (minSizeKB > maxSizeKB || minDuplicate > maxDuplicate || minCoincide > maxCoincide)
				throw new Exception("Wrong configuration value: Minimum is bigger than Maximum");
			
			if (separator.equalsIgnoreCase("\" \""))
				separator = " ";
			else
				separator = "\t";
			res = true;
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(in);
		}
		return res;
	}

	/**
	 * Print self, used for debugging
	 */
	public void printSelf() {
		System.out.println("maxSizeMemory: " + maxSizeMemory);
		System.out.println("numMapper: " + numMapper);
		System.out.println("tmpPath: " + tmpPath);
		System.out.println("launchDelay: " + launchDelay);
		System.out.println("IODelay: " + IODelay);
		System.out.println("transDelay: " + transDelay);
		System.out.println("writeLog: " + writeLog);
		System.out.println("typeEngine: " + typeEngine);
		System.out.println("separator: " + separator);
	}

	/**
	 * Unit test
	 * @param args
	 */
	public static void main(String[] args) {
		String userDir = System.getProperty("user.dir");
		Conf conf = new Conf();
		conf.loadConf(userDir + "/conf.properties");
		conf.printSelf();
	}
}
