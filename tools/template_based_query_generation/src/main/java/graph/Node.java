package graph;

import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Random;

/**
 * class representing node for a node in the markov chain
 */
public class Node<E> {

	private E obj;
	private TreeMap<Double, Node<E>> cumulativeProbabilities;
	private Random r;

	/**
	 * constructs node from E obj and int seed
	 * @param obj
	 */
	public Node(E obj, int seed) {
		this.obj = obj;
		this.r = new Random(seed);
		this.setNeighbors(new HashMap<>());
	}

	/**
	 * constructs node from E obj and Random instance r
	 * @param obj
	 */
	public Node(E obj, Random r) {
		this.obj = obj;
		this.r = r;
		this.setNeighbors(new HashMap<>());
	}

	/**
	 * updates neighborList and cProbabilities when neighbors is changed
	 */
	private void updateProbabilities(HashMap<Node<E>, Double> neighbors) {
		TreeMap<Double, Node<E>> newCumulativeProbabilities = new TreeMap<>();
		if (neighbors.size() != 0) {
			double total = 0;
			for (Node<E> n: neighbors.keySet()) {
				newCumulativeProbabilities.put(total, n);
				total += neighbors.get(n);
			}
		}
		this.cumulativeProbabilities = newCumulativeProbabilities;
	}

	/**
	 *
	 * @return if node is sink
	 */
	public boolean hasNextNode() {
		return (cumulativeProbabilities.size() > 0);
	}

	/**
	 *
	 * @return next random node from current node, returns null if node is sink
	 */
	public Node<E> nextNode() {
		if (this.cumulativeProbabilities.size() == 0) {
			return null;
		}
		return this.cumulativeProbabilities.floorEntry(this.r.nextDouble()).getValue();
	}

	public String toString() {
		return this.obj.toString();
	}

	public E getObj() {
		return this.obj;
	}

	public void setObj(String query) {
		this.obj = obj;
	}


	public void setNeighbors(HashMap<Node<E>, Double> neighbors) {
		this.updateProbabilities(neighbors);
	}

	/**
	 * sets neighbors with
	 * @param neighbors
	 */
	public void setNeighbors(HashSet<Node<E>> neighbors) {
		HashMap<Node<E>, Double> edges = new HashMap<>();
		double c = (neighbors.size() == 0) ? 0 : 1.0/neighbors.size();
		for (Node<E> n: neighbors) {
			edges.put(n, c);
		}
		this.updateProbabilities(edges);
	}

	public TreeMap<Double, Node<E>> getCumulativeProbabilities() {
		return this.cumulativeProbabilities;
	}

}
