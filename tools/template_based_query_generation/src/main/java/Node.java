import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.Random;

/**
 * class representing node for a query in the markov chain
 */
public class Node<E> {

    private E obj;
    private HashMap<Node<E>, Double> neighbors;
    private TreeMap<Double, Node<E>> cumulativeProbabilities;
    private Random r;

    /**
     * constructs node from query
     * @param obj
     */
    public Node(E obj, int seed) {
        this.obj = obj;
        this.r = new Random(seed);
        this.setNeighbors(new HashMap<Node<E>, Double>());
    }

    /**
     * updates neighborList and cProbabilities when neighbors is changed
     */
    private void updateProbabilities() {
        TreeMap<Double, Node<E>> newCumulativeProbabilities = new TreeMap<Double, Node<E>>();
        if (this.neighbors.size() != 0) {
            double total = 0;
            for (Node<E> n: this.neighbors.keySet()) {
                newCumulativeProbabilities.put(total, n);
                total += this.neighbors.get(n);
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

    public HashMap<Node<E>, Double> getNeighbors() {
        return this.neighbors;
    }

    public void setNeighbors(HashMap<Node<E>, Double> neighbors) {
        this.neighbors = neighbors;
        this.updateProbabilities();
    }

    /**
     * sets neighbors with 
     * @param neighbors
     */
    public void setNeighbors(HashSet<Node<E>> neighbors) {
        HashMap<Node<E>, Double> edges = new HashMap<Node<E>, Double>();
        double c = (neighbors.size() == 0) ? 0 : 1.0/neighbors.size();
        for (Node<E> n: neighbors) {
            edges.put(n, c);
        }
        this.neighbors = edges;
        this.updateProbabilities();
    }

    public TreeMap<Double, Node<E>> getCumulativeProbabilities() {
        return this.cumulativeProbabilities;
    }

}