package graph;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.*;

public class MarkovChainTest {

	/**
	 * test for the randomWalk method when the graph consists of isolated vertices (islands)
	 */
	@Test
	public void test_randomWalk_islands() {
		Node<String> node1 = new Node<String>("node 1", 4440);
		Node<String> node2 = new Node<String>("node 2", 2514);
		Node<String> node3 = new Node<String>("node 3", 1913);
		HashSet<Node<String>> nodeSet = new HashSet<Node<String>>();
		nodeSet.add(node1);
		nodeSet.add(node2);
		nodeSet.add(node3);
		MarkovChain mc = new MarkovChain(nodeSet);
		ArrayList<String> walk1 = mc.randomWalk(node1);
		ArrayList<String> walk2 = mc.randomWalk(node2);
		ArrayList<String> walk3 = mc.randomWalk(node3);
		assertEquals(1, walk1.size());
		assertEquals(1, walk2.size());
		assertEquals(1, walk3.size());
		assertEquals(node1.getObj(), walk1.get(0));
		assertEquals(node2.getObj(), walk2.get(0));
		assertEquals(node3.getObj(), walk3.get(0));
	}

	/**
	 * test for the randomWalk method when the graph consists of one directed edge
	 */
	@Test
	public void test_randomWalk_anEdge() {
		Node<String> node1 = new Node<String>("node 1", 3408);
		Node<String> node2 = new Node<String>("node 2", 9642);
		HashSet<Node<String>> node1Neighbors = new HashSet<Node<String>>();
		node1Neighbors.add(node2);
		node1.setNeighbors(node1Neighbors);
		HashSet<Node<String>> nodeSet = new HashSet<Node<String>>();
		nodeSet.add(node1);
		nodeSet.add(node2);
		MarkovChain mc = new MarkovChain(nodeSet);
		ArrayList<String> walk1 = mc.randomWalk(node1);
		ArrayList<String> walk2 = mc.randomWalk(node2);
		assertEquals(2, walk1.size());
		assertEquals(1, walk2.size());
		assertEquals(node1.getObj(), walk1.get(0));
		assertEquals(node2.getObj(), walk1.get(1));
		assertEquals(node2.getObj(), walk2.get(0));
	}

	/**
	 * test for the randomWalk method when the graph consists of a bidirectional edge
	 * an infinite loop happens in the graph.MarkovChain class, so we expect an OutOfMemoryError.class
	 */
	//@Test
	public void test_randomWalk_nonDAG() {
		Node<String> node1 = new Node<String>("node 1", 3408);
		Node<String> node2 = new Node<String>("node 2", 9642);
		HashSet<Node<String>> node1Neighbors = new HashSet<Node<String>>();
		node1Neighbors.add(node2);
		node1.setNeighbors(node1Neighbors);
		HashSet<Node<String>> node2Neighbors = new HashSet<Node<String>>();
		node2Neighbors.add(node1);
		node2.setNeighbors(node2Neighbors);
		HashSet<Node<String>> nodeSet = new HashSet<Node<String>>();
		nodeSet.add(node1);
		nodeSet.add(node2);
		MarkovChain mc = new MarkovChain(nodeSet);
		assertThrows(OutOfMemoryError.class, () -> {
			ArrayList<String> walk1 = mc.randomWalk(node1);
		});
	}

	/**
	 * test for the randomWalk method when the graph is a small DAG
	 */
	@Test
	public void test_randomWalk_smallDAG() {
		Random r = new Random();
		Node<String> node1 = new Node<String>("node 1", 6033);
		Node<String> node2 = new Node<String>("node 2", 8509);
		Node<String> node3 = new Node<String>("node 3", 5991);
		Node<String> node4 = new Node<String>("node 4", 3900);
		Node<String> node5 = new Node<String>("node 5", 3679);
		HashMap<Node<String>, HashMap<Node<String>, Double>> nodeMap = new HashMap<Node<String>, HashMap<Node<String>, Double>>();
		HashMap<Node<String>, Double> node1Neighbors = new HashMap<Node<String>, Double>();
		HashMap<Node<String>, Double> node2Neighbors = new HashMap<Node<String>, Double>();
		HashMap<Node<String>, Double> node3Neighbors = new HashMap<Node<String>, Double>();
		HashMap<Node<String>, Double> node4Neighbors = new HashMap<Node<String>, Double>();
		node1Neighbors.put(node2, 0.40);
		node1Neighbors.put(node3, 0.40);
		node1Neighbors.put(node5, 0.20);
		node2Neighbors.put(node4, 1.0);
		node3Neighbors.put(node4, 1.0);
		node4Neighbors.put(node5, 1.0);
		nodeMap.put(node1, node1Neighbors);
		nodeMap.put(node2, node2Neighbors);
		nodeMap.put(node3, node3Neighbors);
		nodeMap.put(node4, node4Neighbors);
		MarkovChain mc = new MarkovChain(nodeMap);
		int numShortPaths = 0;
		for (int i = 0; i < 1000000; i++) {
			ArrayList<String> walk = mc.randomWalk(node1);
			if (walk.size() == 2) {
				numShortPaths++;
				assertEquals("node 1", walk.get(0));
				assertEquals("node 5", walk.get(1));
			} else if (walk.size() == 4) {
				assertEquals("node 1", walk.get(0));
				if (!walk.get(1).equals("node 2") && !walk.get(1).equals("node 3")) {
					fail("Bad random walk");
				}
				assertEquals("node 4", walk.get(2));
				assertEquals("node 5", walk.get(3));
			} else {
				System.out.println(walk);
				fail("Walk of inappropriate length");
			}
		}
		// to fail we need to be greater than 100 standard deviations away from mean
		assertEquals(0.20, numShortPaths/1000000., 0.10);
	}

}
