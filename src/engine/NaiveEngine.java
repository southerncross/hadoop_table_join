package engine;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import model.CostModel;

import org.apache.hadoop.io.IOUtils;

import data.Conf;
import data.Method;
import data.Node;
import data.Table;

/**
 * Optimal engine which is used in multiple table join process
 * 
 * Sequential order(no optimization)
 * 
 * Extends OptimalEngine class
 * 
 * @author lishunyang
 */
public class NaiveEngine extends OptimalEngine {

	private FileOutputStream fos;
	private PrintStream ps;
	private StringBuffer sb;

	public NaiveEngine(Conf conf) {
		super.conf = conf;
		super.model = new CostModel(conf);

		// log
		sb = new StringBuffer();
		sb.append(">>Naive Engine<<\n");
		if (conf.writeLog) {
			try {
				fos = new FileOutputStream("log");
				ps = new PrintStream(fos);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}
	}

	public void multiJoin(List<Node> joinList, String out) throws Exception {
		Node executeTree;

		while (joinList.size() > 1)
			sequenceAndJoin(joinList, out);
		executeTree = joinList.get(0);
		drawExecutionTree(executeTree);

		if (conf.writeLog) {
			ps.print(sb.toString());
			if (fos != null)
				IOUtils.closeStream(fos);
		}
	}

	/**
	 * Join tables in sequential order
	 * @param joinList
	 * @param out
	 * @throws Exception
	 */
	private void sequenceAndJoin(List<Node> joinList, String out)
			throws Exception {
		Node left, right, result;
		int optIndex = 0;
		Method method;

		result = new Node();
		left = joinList.remove(optIndex);
		right = joinList.remove(optIndex);
		model.sampling(left.table);
		model.sampling(right.table);
		result.table = model.estimateJoinTable(left.table, right.table);
		method = model.selectMethod(left.table, right.table);
		if (!joinList.isEmpty())
			out = conf.tmpPath.toString() + "/" + (joinList.size() + 2) + "-"
					+ left.table.path.getName() + right.table.path.getName();
		result = new Node(new Table(out + "/part-00000"));
		result.name = left.name + right.name;
		result.leftChild = left;
		result.rightChild = right;
		joinList.add(optIndex, result); // single reducer

		sb.append(left.name + " " + right.name + ": " + method.name() + "\n");
		tableJoin(left.table.path.toString(), right.table.path.toString(), out,
				method);
	}

	@Override
	public void drawExecutionTree(Node root) {
		List<Node> layer1, layer2;
		Node node;

		layer1 = new LinkedList<Node>();
		layer2 = new LinkedList<Node>();
		layer1.add(root);

		while (!layer1.isEmpty() || !layer2.isEmpty()) {
			while (!layer1.isEmpty()) {
				node = layer1.remove(0);
				sb.append(node.name);
				sb.append("(");
				if (node.leftChild != null) {
					layer2.add(node.leftChild);
					sb.append("l");
				}
				if (node.rightChild != null) {
					layer2.add(node.rightChild);
					sb.append("r");
				}
				sb.append(") ");
			}
			sb.append("\n");
			while (!layer2.isEmpty()) {
				node = layer2.remove(0);
				sb.append(node.name);
				sb.append("(");
				if (node.leftChild != null) {
					layer1.add(node.leftChild);
					sb.append("l");
				}
				if (node.rightChild != null) {
					layer1.add(node.rightChild);
					sb.append("r");
				}
				sb.append(") ");
			}
			sb.append("\n");
		}
	}
}
