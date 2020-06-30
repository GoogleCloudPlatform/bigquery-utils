import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;


public class NodeTest {

    @Test
    public void test_getNeighborList() {
        Node<String> node1 = new Node<String>("node 1", 8535);
        Node<String> node2 = new Node<String>("node 2", 8580);
        Node<String> node3 = new Node<String>("node 3", 3488);
        Node<String> node4 = new Node<String>("node 4", 8566);
        HashSet<Node<String>> node1Neighbors = new HashSet<Node<String>>();
        node1Neighbors.add(node2);
        node1Neighbors.add(node3);
        node1Neighbors.add(node4);
        node1.setNeighbors(node1Neighbors);
        ArrayList<Node<String>> neighborList = node1.getNeighborList();
        assertEquals(-1, neighborList.indexOf(node1));
        assertEquals(3, neighborList.size());
        assertNotEquals(-1, neighborList.indexOf(node2));
        assertNotEquals(-1, neighborList.indexOf(node3));
        assertNotEquals(-1, neighborList.indexOf(node4));
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
        Node<Integer> node1 = new Node<Integer>(31, 4767);
        Node<Integer> node2 = new Node<Integer>(-12, 3556);
        Node<Integer> node3 = new Node<Integer>(0, 8924);
        Node<Integer> node4 = new Node<Integer>(7777, 4490);
        HashSet<Node<Integer>> node1Neighbors = new HashSet<Node<Integer>>();
        node1Neighbors.add(node2);
        node1Neighbors.add(node3);
        node1Neighbors.add(node4);
        node1.setNeighbors(node1Neighbors);
        ArrayList<Double> cProbabilities = node1.getCProbabilities();
        assertEquals(0, cProbabilities.get(0));
        assertEquals(0.3333333, cProbabilities.get(1), 0.01);
        assertEquals(0.6666666, cProbabilities.get(2), 0.01);
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
        ArrayList<Double> cProbabilities = node1.getCProbabilities();
        cProbabilities.set(0, cProbabilities.get(1)-cProbabilities.get(0));
        cProbabilities.set(1, cProbabilities.get(2)-cProbabilities.get(1));
        cProbabilities.set(2, 1-cProbabilities.get(2));
        Collections.sort(cProbabilities);
        assertEquals(0.15, cProbabilities.get(0), 0.01);
        assertEquals(0.25, cProbabilities.get(1), 0.01);
        assertEquals(0.60, cProbabilities.get(2), 0.01);    }

    @Test
    public void test_nextNode() {
        Node<Integer> node1 = new Node<Integer>(31, 8987);
        Node<Integer> node2 = new Node<Integer>(-12, 7620);
        Node<Integer> node3 = new Node<Integer>(1, 1563);
        Node<Integer> node4 = new Node<Integer>(7777, 7087);
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