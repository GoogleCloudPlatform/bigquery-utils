import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.ArrayList;
import java.util.Random;

public class Node {

    private String query;
    private HashMap<Node, Double> neighbors;
    private ArrayList<Node> neighborList;
    private ArrayList<Double> cprobabilities;
    private Random r = new Random(314);


    public Node() {
        this.query = "";
        this.setNeighbors(new HashMap<Node, Double>());
    }

    public Node(String query) {
        this.query = query;
        this.setNeighbors(new HashMap<Node, Double>());
    }

    public Node(String query, HashSet<Node> nodeSet){
        this.query = query;
        this.setNeighbors(nodeSet);
    }

    public Node(String query, HashMap<Node, Double> neighbors){
        this.query = query;
        this.setNeighbors(neighbors);
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

    public void setNeighbors(HashSet<Node> neighbors) {
        HashMap<Node, Double> edges = new HashMap<Node, Double>();
        double c = (neighbors.size() == 0) ? 0 : 1.0/neighbors.size();
        for (Node n: neighbors) {
            edges.put(n, c);
        }
        this.neighbors = edges;
        this.updateProbabilities();
    }

    public ArrayList<Node> getNeighborList() {
        return this.neighborList;
    }

    public ArrayList<Double> getCprobabilities() {
        return this.cprobabilities;
    }

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

    public boolean hasNextNode() {
        return (neighbors.size() > 0);
    }

    public Node nextNode() {
        if (this.neighborList.size() == 0) {
            return null;
        }
        double randDouble = this.r.nextDouble();
        // System.out.println(randDouble);
        // find largeset index such that cprob is less than randDouble
        // int low = 0, high = this.neighborList.size();
        // while (high - low > 1) {
        //     int mid = (low + high) / 2;
        //     if (this.cprobabilities.get(mid) > randDouble) {
        //         high = mid;
        //     } else {
        //         low = mid;
        //     }
        // }
        for (int i = 0; i < this.neighborList.size(); i ++) {
            if (this.cprobabilities.get(i) > randDouble) return (this.neighborList.get(i-1));
        }
        return this.neighborList.get(this.neighborList.size()-1);
    }

}