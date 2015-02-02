package engine;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.LinkedList;
import java.util.List;

import model.CostModel;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import data.Conf;
import data.Method;
import data.Node;
import data.Table;

/**
 * Optimal engine which is used in multiple table join process
 * 
 * Dynamic programming strategy
 * 
 * Extends OptimalEngine class
 * 
 * @author lishunyang
 * 
 */
public class DPEngine extends OptimalEngine {

	private FileOutputStream fos;
	private PrintStream ps;
	private StringBuffer sb;

	public DPEngine(Conf conf) {
		super.conf = conf;
		super.model = new CostModel(conf);

		// log
		sb = new StringBuffer();
		sb.append(">>DP engine<<\n");
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
		Node executeTree = estimateAndBuild(joinList);
		drawExecutionTree(executeTree);
		execute(executeTree, out);

		if (conf.writeLog) {
			ps.print(sb.toString());
			if (fos != null)
				IOUtils.closeStream(fos);
		}
	}

	/**
	 * Estimate cost and build execution tree
	 * 
	 * @param joinList
	 * @return
	 * @throws Exception
	 */
	private Node estimateAndBuild(List<Node> joinList) throws Exception {
		int n = joinList.size(); // number of tables
		int i, j, k;
		long[][] cost = new long[n][n]; // join cost
		Table[][] tables = new Table[n][n]; // table size
		int[][] track = new int[n][n]; // track record
		Method[][] methods = new Method[n][n];
		long costIJ, costTmp;
		Method methodIJ, methodTmp;
		Table tableIJ, tableTmp;
		int indexK;
		Node executeTree;

		for (i = 0; i < n; i++) {
			cost[i][i] = 0;
			tables[i][i] = joinList.get(i).table;
			model.sampling(tables[i][i]);
		}

		for (int r = 2; r <= n; r++) {
			for (i = 0; i <= n - r; i++) {
				j = i + r - 1;
				costIJ = -1;
				methodIJ = null;
				tableIJ = null;
				indexK = -1;
				for (k = i; k < j; k++) {
					methodTmp = model.selectMethod(tables[i][k],
							tables[k + 1][j]);
					tableTmp = model.estimateJoinTable(tables[i][k],
							tables[k + 1][j]);
					costTmp = cost[i][k]
							+ cost[k + 1][j]
							+ model.estimateCost(tables[i][k],
									tables[k + 1][j], tableTmp, methodTmp);
					if (costIJ < 0 || costTmp < costIJ) {
						costIJ = costTmp;
						methodIJ = methodTmp;
						tableIJ = tableTmp;
						indexK = k;
					}
				}
				tables[i][j] = tableIJ;
				cost[i][j] = costIJ;
				track[i][j] = indexK;
				methods[i][j] = methodIJ;
			}
		}

		executeTree = buildExecuteTree(joinList, track, methods, 0, n - 1);
		executeTree.name = "root"; // for simplify the judgment during execution

		return executeTree;
	}

	/**
	 * Build execution tree
	 * 
	 * @param joinList
	 * @param track
	 * @param methods
	 * @param i
	 * @param j
	 * @return
	 */
	private Node buildExecuteTree(List<Node> joinList, int[][] track,
			Method[][] methods, int i, int j) {
		Node root = null;
		if (i == j) {
			root = joinList.get(i);
		} else {
			root = new Node();
			root.method = methods[i][j];
			root.leftChild = buildExecuteTree(joinList, track, methods, i,
					track[i][j]);
			root.rightChild = buildExecuteTree(joinList, track, methods,
					track[i][j] + 1, j);
			root.name = root.leftChild.name + root.rightChild.name;
		}
		return root;
	}

	/**
	 * Execute join process according to execution tree
	 * 
	 * @param root
	 * @param out
	 * @throws ClassNotFoundException
	 * @throws IOException
	 * @throws InterruptedException
	 */
	private void execute(Node root, String out) throws ClassNotFoundException,
			IOException, InterruptedException {
		String outPath;

		// leaf node, do nothing
		if (null == root.leftChild && null == root.rightChild)
			return;
		execute(root.leftChild, out);
		execute(root.rightChild, out);
		outPath = conf.tmpPath.toString() + "/" + root.name;
		root.table.path = new Path(outPath + "/part-00000");
		if (root.name.equalsIgnoreCase("root"))
			outPath = out;
		sb.append(root.leftChild.name + " " + root.rightChild.name + ": "
				+ root.method.name() + "\n");
		tableJoin(root.leftChild.table.path.toString(),
				root.rightChild.table.path.toString(), outPath, root.method);
		root.leftChild = root.rightChild = null; // change this node to leaf
													// node
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
