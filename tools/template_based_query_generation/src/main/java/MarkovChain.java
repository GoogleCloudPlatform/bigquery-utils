
import java.util.ArrayList;
import java.util.HashSet;
import java.util.HashMap;
import java.util.Random;

/**
 * Class that represents src.main.MarkovChain encoding query and keyword dependencies
 */
public class MarkovChain<E> {
    
    private HashSet<Node<E>> nodes;
    private Random r;

    /**
     * constructs MarkovChain object from edge weights and random seed
     * @param nodes
     * @param seed
     */
    public MarkovChain(HashMap<Node<E>, HashMap<Node<E>, Double>> nodes, int seed) {
        this.r = new Random(seed);
        this.nodes = (HashSet) nodes.keySet();
        for (Node<E> n: nodes.keySet()){
            n.setNeighbors(nodes.get(n));
        }
    }

    /**
     * constructs MarkovChain object from set of nodes and random seed
     * @param nodes
     * @param seed
     */
    public MarkovChain(HashSet<Node<E>> nodes, int seed) {
        this.r = new Random(seed);
        this.nodes = nodes;
    }

    /**
     * 
     * @param start
     * @return list of nodes for a random walk from start node
     */
    public ArrayList<E> randomWalk(Node<E> start) {
        ArrayList<E> walk = new ArrayList<E>();
        Node<E> current = start;
        while (current.hasNextNode()) {
            walk.add(current.getObj());
            current = current.nextNode();
        }
        walk.add(current.getObj());
        return walk;
    }

}