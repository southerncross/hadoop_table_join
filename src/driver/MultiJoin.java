package driver;

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import data.Conf;
import data.Node;
import data.Table;
import engine.DPEngine;
import engine.GreedyEngine;
import engine.NaiveEngine;
import engine.OptimalEngine;

/**
 * Driver of MultiJoin
 * 
 * Implements JoinDriver interface
 * 
 * @author lishunyang
 * 
 */
public class MultiJoin implements JoinDriver {

	/**
	 * Unite test
	 * 
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.err
					.println("Usage: Join <table path> <table path> [<table path>]* <output path>");
			System.exit(-1);
		}
		String userDir = System.getProperty("user.dir");
		Conf conf = new Conf();
		if (!conf.loadConf(userDir + "/conf.properties")) {
			System.err.println("Failed in loading configuration file, exit");
			System.exit(-2);
		}

		new MultiJoin().join(args, conf);
	}

	public void join(String[] args, Conf conf) throws Exception {
		OptimalEngine oe;

		if (conf.typeEngine.equalsIgnoreCase("Greedy"))
			oe = new GreedyEngine(conf); // Greedy method
		else if (conf.typeEngine.equalsIgnoreCase("DP"))
			oe = new DPEngine(conf); // Dynamic Programming method
		else if (conf.typeEngine.equalsIgnoreCase("Naive"))
			oe = new NaiveEngine(conf); // Naive Engine
		else
			throw new Exception("Unknown engine type: " + conf.typeEngine);
		FileSystem hdfs = new Path(args[0]).getFileSystem(new Configuration());
		List<Node> joinList = generateJoinList(args);
		String out = args[args.length - 1];
		try {
			oe.multiJoin(joinList, out);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			hdfs.delete(conf.tmpPath, true);
		}
	}

	/**
	 * Change args to table list
	 * 
	 * @param args
	 * @return
	 */
	private List<Node> generateJoinList(String[] args) {
		List<Node> nodes = new LinkedList<Node>();
		Node node;
		int numArgs = args.length;

		for (int i = 0; i < numArgs - 1; i++) {
			node = new Node(new Table(args[i]));
			node.name = new Path(args[i]).getName();
			nodes.add(node);
		}
		return nodes;
	}
}
