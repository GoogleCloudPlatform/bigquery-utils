package graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

/**
 * Class that represents src.main.graph.MarkovChain encoding query and keyword dependencies
 */
public class MarkovChain<E> {

  private HashSet<Node<E>> nodes;

  /**
   * constructs graph.MarkovChain object from edge weights and random seed
   *
   * @param nodes
   */
  public MarkovChain(HashMap<Node<E>, HashMap<Node<E>, Double>> nodes) {
    this.nodes = new HashSet<>();
    this.nodes.addAll(nodes.keySet());
    for (Node<E> n : nodes.keySet()) {
      n.setNeighbors(nodes.get(n));
    }
  }

  /**
   * constructs graph.MarkovChain object from set of nodes and random seed
   *
   * @param nodes
   */
  public MarkovChain(HashSet<Node<E>> nodes) {
    this.nodes = nodes;
  }

  /**
   * @param start
   * @return list of nodes for a random walk from start node
   */
  public ArrayList<E> randomWalk(Node<E> start) {
    ArrayList<E> walk = new ArrayList<>();
    Node<E> current = start;
    while (current.hasNextNode()) {
      walk.add(current.getObj());
      current = current.nextNode();
    }
    walk.add(current.getObj());
    return walk;
  }

}
