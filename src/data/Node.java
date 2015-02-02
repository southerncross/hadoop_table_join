package data;

/**
 * Execution tree node, used in multiple join
 * 
 * @author lishunyang
 * 
 */
public class Node {
	public String name;
	public Node leftChild;
	public Node rightChild;
	public Table table;
	public Method method;

	public Node() {
		table = new Table();
	}

	public Node(Table table) {
		this.table = table;
	}
}
