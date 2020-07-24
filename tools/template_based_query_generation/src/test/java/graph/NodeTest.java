package graph;

import org.junit.jupiter.api.Test;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

public class NodeTest {

	@Test
	public void test_getNeighborList() {
		Random r = new Random();
		Node<String> node1 = new Node<String>("node 1", r);
		Node<String> node2 = new Node<String>("node 2", r);
		Node<String> node3 = new Node<String>("node 3", r);
		Node<String> node4 = new Node<String>("node 4", r);
		HashSet<Node<String>> node1Neighbors = new HashSet<Node<String>>();
		node1Neighbors.add(node2);
		node1Neighbors.add(node3);
		node1Neighbors.add(node4);
		node1.setNeighbors(node1Neighbors);
		HashSet<Node<String>> neighborSet = new HashSet<Node<String>>();
		neighborSet.addAll(node1.getCumulativeProbabilities().values());
		assertEquals(false, neighborSet.contains(node1));
		assertEquals(3, neighborSet.size());
		assertNotEquals(false, neighborSet.contains(node2));
		assertNotEquals(false, neighborSet.contains(node3));
		assertNotEquals(false, neighborSet.contains(node4));
	}

	@Test
	public void test_hasNextNode() {
		Node<Integer> node1 = new Node<Integer>(31, 1626);
		Node<Integer> node2 = new Node<Integer>(-12, 9443);
		Node<Integer> node3 = new Node<Integer>(0, 9377);
		Node<Integer> node4 = new Node<Integer>(7777, 6292);
		HashSet<Node<Integer>> node1Neighbors = new HashSet<Node<Integer>>();
		HashSet<Node<Integer>> node2Neighbors = new HashSet<Node<Integer>>();
		HashSet<Node<Integer>> node3Neighbors = new HashSet<Node<Integer>>();
		node1Neighbors.add(node2);
		node1Neighbors.add(node3);
		node1Neighbors.add(node4);
		node2Neighbors.add(node4);
		node1.setNeighbors(node1Neighbors);
		node2.setNeighbors(node2Neighbors);
		node3.setNeighbors(node3Neighbors);
		// node3 has neighbors set to empty HashSet while node 4 doesn't have neighbors set
		assertEquals(true, node1.hasNextNode());
		assertEquals(true, node2.hasNextNode());
		assertEquals(false, node3.hasNextNode());
		assertEquals(false, node4.hasNextNode());
	}

	@Test
	public void test_setNeighbors_set() {
		Random r = new Random();
		Node<Integer> node1 = new Node<Integer>(31, r);
		Node<Integer> node2 = new Node<Integer>(-12, r);
		Node<Integer> node3 = new Node<Integer>(0, r);
		Node<Integer> node4 = new Node<Integer>(7777, r);
		HashSet<Node<Integer>> node1Neighbors = new HashSet<Node<Integer>>();
		node1Neighbors.add(node2);
		node1Neighbors.add(node3);
		node1Neighbors.add(node4);
		node1.setNeighbors(node1Neighbors);
		TreeMap<Double, Node<Integer>> cumulativeProbabilities = node1.getCumulativeProbabilities();
		assertEquals(0, cumulativeProbabilities.firstEntry().getKey());
		assertEquals(0.6666666, cumulativeProbabilities.lastEntry().getKey(), 0.01);
	}

	@Test
	public void test_setNeighbors_map() {
		Node<Integer> node1 = new Node<Integer>(31, 2159);
		Node<Integer> node2 = new Node<Integer>(0, 7926);
		Node<Integer> node3 = new Node<Integer>(877, 8677);
		Node<Integer> node4 = new Node<Integer>(7777, 9139);
		HashMap<Node<Integer>, Double> node1Neighbors = new HashMap<Node<Integer>, Double>();
		node1Neighbors.put(node2, 0.15);
		node1Neighbors.put(node3, 0.25);
		node1Neighbors.put(node4, 0.60);
		node1.setNeighbors(node1Neighbors);
		ArrayList<Double> cumulativeProbabilities = new ArrayList<Double>();
		cumulativeProbabilities.addAll(node1.getCumulativeProbabilities().keySet());
		Collections.sort(cumulativeProbabilities);
		ArrayList<Double> probabilities = new ArrayList<Double>();
		probabilities.add(cumulativeProbabilities.get(1));
		probabilities.add(cumulativeProbabilities.get(2) - cumulativeProbabilities.get(1));
		probabilities.add(1 - cumulativeProbabilities.get(2));
		Collections.sort(probabilities);
		assertEquals(0.15, probabilities.get(0), 0.01);
		assertEquals(0.25, probabilities.get(1), 0.01);
		assertEquals(0.60, probabilities.get(2), 0.01);
	}

	@Test
	public void test_nextNode() {
		Random r = new Random();
		Node<Integer> node1 = new Node<Integer>(31, r);
		Node<Integer> node2 = new Node<Integer>(-12, r);
		Node<Integer> node3 = new Node<Integer>(1, r);
		Node<Integer> node4 = new Node<Integer>(7777, r);
		HashSet<Node<Integer>> node1Neighbors = new HashSet<Node<Integer>>();
		HashSet<Node<Integer>> node2Neighbors = new HashSet<Node<Integer>>();
		node1Neighbors.add(node2);
		node1Neighbors.add(node3);
		node2Neighbors.add(node4);
		node1.setNeighbors(node1Neighbors);
		node2.setNeighbors(node2Neighbors);
		// get next node from node1 and check its only node2 and node3
		for (int i = 0; i < 20; i++) {
			int n = (int) node1.nextNode().getObj();
			if (n != -12 && n != 1) {
				fail("test_nextNode failed for nextNode of node1");
			}
		}
		for (int i = 0; i < 20; i++) {
			int n = (int) node2.nextNode().getObj();
			if (n != 7777) {
				fail("test_nextNode failed for nextNode of node2");
			}
		}

	}

}