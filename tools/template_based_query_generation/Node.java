import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.Random;

/**
 * class representing node for a query in the markov chain
 */
public class Node {

    private String query;
    private HashMap<Node, Double> neighbors;
    private ArrayList<Node> neighborList; // list of neighbors and corresponding cumulative probabilities
    private ArrayList<Double> cprobabilities; // cumulative probabilities
    private Random r = new Random(314);

    /**
     * constructs node from query
     * @param query
     */
    public Node(String query) {
        this.query = query;
        this.setNeighbors(new HashMap<Node, Double>());
    }

    /**
     * constructs node from query, and set of neighbors which can be reached with equiprobability
     * @param query
     * @param nodeSet
     */
    public Node(String query, HashSet<Node> neighbors){
        this.query = query;
        this.setNeighbors(neighbors);
    }

    /**
     * constructs node from query, and map of neighbors and transition probabilities
     * @param query
     * @param neighbors
     */
    public Node(String query, HashMap<Node, Double> neighbors) {
        this.query = query;
        this.setNeighbors(neighbors);
    }

    /**
     * updates neighborList and cprobabilities when neighbors is changed
     */
    private void updateProbabilities() {
        if (this.neighbors.size() != 0) {
            Set<Node> neighborSet = this.neighbors.keySet();
            double total = 0;
            ArrayList<Node> newNeighborList = new ArrayList<Node>();
            ArrayList<Double> newCprobabilities = new ArrayList<Double>();
            for (Node n: neighborSet) {
                newNeighborList.add(n);
                newCprobabilities.add(total);
                total += this.neighbors.get(n);
            }
            this.neighborList = newNeighborList;
            this.cprobabilities = newCprobabilities;
        } else {
            this.neighborList = new ArrayList<Node>();
            this.cprobabilities = new ArrayList<Double>();
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

        // find largeset index such that cprob is less than randDouble
        int low = 0, high = this.neighborList.size();
        while (high - low > 1) {
            int mid = (low + high) / 2;
            if (this.cprobabilities.get(mid) > randDouble) {
                high = mid;
            } else {
                low = mid;
            }
        }
        return this.neighborList.get(low);
    }

    public String toString() {
        return this.query;
    }

    public String getQuery() {
        return this.query;
    }

    public void setQuery(String query) {
        this.query = query;
    }

    public HashMap<Node, Double> getNeighbors() {
        return this.neighbors;
    }

    public void setNeighbors(HashMap<Node, Double> neighbors) {
        this.neighbors = neighbors;
        this.updateProbabilities();
    }

    /**
     * sets neighbors with 
     * @param neighbors
     */
    public void setNeighbors(HashSet<Node> neighbors) {
        HashMap<Node, Double> edges = new HashMap<Node, Double>();
        double c = (neighbors.size() == 0) ? 0 : 1.0/neighbors.size();
        for (Node n: neighbors) {
            edges.put(n, c);
        }
        this.neighbors = edges;
        this.updateProbabilities();
    }

}