package utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.io.IOUtils;

import data.Conf;

/**
 * Used for generating test data
 * 
 * @author lishunyang
 * 
 */
public class Test {

	private Conf conf;

	public static void main(String[] args) throws NumberFormatException,
			IOException {
		if (args.length != 2) {
			System.err.println("Usage: Generate <file name> [number of file]");
			System.exit(-1);
		}
		String userDir = System.getProperty("user.dir");
		Conf conf = new Conf();
		if (!conf.loadConf(userDir + "/conf.properties")) {
			System.err.println("Failed in loading configuration file, exit");
			System.exit(-2);
		}

		Test test = new Test();
		test.conf = conf;
		test.generateRandomFile(args[0], Integer.valueOf(args[1]));
	}

	/**
	 * Given file name and size, generate data file filled of random values
	 * 
	 * @param fileName
	 * @param minSize
	 * @param maxSize
	 * @throws IOException
	 */
	public void generateRandomFile(String fileName, int n) throws IOException {
		Random ran = new Random();
		long coreSize = 0;
		int coreNumTuple = 0;
		long[] size;
		int[] numTuple;
		int[] counter;
		int[] duplicate;
		int[] coincide;
		String[] line;
		File[] f;
		FileWriter[] fw;
		String userDir;
		int flag = 1;

		userDir = System.getProperty("user.dir");
		size = new long[n];
		numTuple = new int[n];
		counter = new int[n];
		duplicate = new int[n];
		coincide = new int[n];
		line = new String[n + 1]; // col
		f = new File[n];
		fw = new FileWriter[n];

		if (conf.maxSizeKB == conf.minSizeKB)
			coreSize = conf.minSizeKB;
		else
			coreSize = conf.minSizeKB + Math.abs(ran.nextInt())
					% (conf.maxSizeKB - conf.minSizeKB);
		coreSize <<= 10;
		for (int i = 0; i < n; i++) {
			line[i] = UUID.randomUUID().toString();
			if (conf.maxDuplicate == conf.minDuplicate)
				duplicate[i] = conf.minDuplicate;
			else
				duplicate[i] = conf.minDuplicate + Math.abs(ran.nextInt())
						% (conf.maxDuplicate - conf.minDuplicate + 1);
			if (conf.minCoincide == conf.maxCoincide)
				coincide[i] = conf.minCoincide;
			else
				coincide[i] = conf.minCoincide + Math.abs(ran.nextInt())
						% (conf.maxCoincide - conf.minCoincide + 1);
			size[i] = coreSize * 100 / coincide[i] * duplicate[i];
			numTuple[i] = (int) (size[i] / (long) line[i].length() / (long) 2);
			coreNumTuple = (int) (coreSize / (long) line[i].length() / (long) 2);
			counter[i] = 0;
			f[i] = new File(userDir + "/" + fileName + "_" + i);
			fw[i] = new FileWriter(f[i]);
		}

		try {
			// core section
			for (int i = 0; i < coreNumTuple; i++) { // every tuple
				for (int j = 0; j <= n; j++)
					line[j] = UUID.randomUUID().toString();
				for (int j = 0; j < n; j++) { // every file
					for (int k = 0; k < duplicate[j]; k++) { // duplicate
						fw[j].write(line[j] + conf.separator + k
								+ conf.separator + line[j + 1] + "\n");
						counter[j]++;
					}
				}
			}
			// redundant section
			flag = 1;
			while (flag != 0) {
				flag = 0;
				for (int i = 0; i < n; i++) {
					if (counter[i] < numTuple[i]) {
						flag = 1;
						for (int j = 0; j < duplicate[i]; j++) {
							fw[i].write(UUID.randomUUID().toString()
									+ conf.separator + j + conf.separator
									+ UUID.randomUUID().toString() + "\n");
							counter[i]++;
						}
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			for (int i = 0; i < n; i++)
				IOUtils.closeStream(fw[i]);
		}
	}
}
