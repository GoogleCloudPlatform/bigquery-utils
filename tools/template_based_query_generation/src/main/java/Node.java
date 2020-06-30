import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.Random;

/**
 * class representing node for a query in the markov chain
 */
public class Node<E> {

    private E obj;
    private HashMap<Node<E>, Double> neighbors;
    private ArrayList<Node<E>> neighborList; // list of neighbors and corresponding cumulative probabilities
    private ArrayList<Double> cProbabilities; // cumulative probabilities
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
        if (this.neighbors.size() != 0) {
            Set<Node<E>> neighborSet = this.neighbors.keySet();
            double total = 0;
            ArrayList<Node<E>> newNeighborList = new ArrayList<Node<E>>();
            ArrayList<Double> newCProbabilities = new ArrayList<Double>();
            for (Node<E> n: neighborSet) {
                newNeighborList.add(n);
                newCProbabilities.add(total);
                total += this.neighbors.get(n);
            }
            this.neighborList = newNeighborList;
            this.cProbabilities = newCProbabilities;
        } else {
            this.neighborList = new ArrayList<Node<E>>();
            this.cProbabilities = new ArrayList<Double>();
        }
    }

    /**
     * 
     * @return if node is sink
     */
    public boolean hasNextNode() {
        return (neighbors.size() > 0);
    }

    /**
     * 
     * @return next random node from current node, returns null if node is sink
     */
    public Node nextNode() {
        if (this.neighborList.size() == 0) {
            return null;
        }
        double randDouble = this.r.nextDouble();

        // find largest index such that cProbabilities is less than randDouble
        int low = 0, high = this.neighborList.size();
        while (high - low > 1) {
            int mid = (low + high) / 2;
            if (this.cProbabilities.get(mid) > randDouble) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return this.neighborList.get(low);
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

    public ArrayList<Node<E>> getNeighborList() {
        return neighborList;
    }

    public ArrayList<Double> getCProbabilities() {
        return cProbabilities;
    }

}