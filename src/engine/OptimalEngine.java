package engine;

import java.io.IOException;
import java.util.List;

import model.CostModel;

import data.Conf;
import data.Method;
import data.Node;
import driver.CopyJoin;
import driver.NormalJoin;
import driver.SemiJoin;

/**
 * Optimal engine which is used in multiple tables join
 * 
 * @author lishunyang
 * 
 */
public abstract class OptimalEngine {

	protected Conf conf; // configuration
	protected CostModel model; // cost model

	abstract public void multiJoin(List<Node> joinList, String out)
			throws Exception;

	public void drawExecutionTree(Node root) {
		System.out
				.println("No execution tree can be shown, draw method hasn't been overrided yet.");
	}

	protected void tableJoin(String leftPath, String rightPath, String outPath,
			Method method) throws ClassNotFoundException, IOException,
			InterruptedException {
		String[] args = new String[3];

		args[0] = leftPath;
		args[1] = rightPath;
		args[2] = outPath;

		switch (method) {
		case NORMALJOIN:
			new NormalJoin().join(args, conf);
			;
			break;
		case COPYJOIN:
			new CopyJoin().join(args, conf);
			break;
		case SEMIJOIN:
			new SemiJoin().join(args, conf);
			break;
		}
	}
}
