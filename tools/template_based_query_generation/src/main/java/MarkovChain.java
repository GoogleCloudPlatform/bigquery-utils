
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.FileReader;

/**
 * Class that represents src.main.MarkovChain encoding query and keyword dependencies
 */
public class MarkovChain {
    
    private HashMap<String, Node> nodes;
    private Node root;

    /**
     * Constructs src.main.MarkovChain from dependencies.txt
     * @param filename
     * @param root
     * @throws Exception
     */
    public MarkovChain(String filename, String root) throws Exception {
        // read in lines from filename, ignoring lines that begin with ' ' or '/'
        // stores directed edges in dependencies
        HashMap<String, HashSet<String>> dependencies = new HashMap<String, HashSet<String>>();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.length() > 2 && line.charAt(0) != ' ' 
                && !(line.charAt(0) == '/' && line.charAt(1) == '/')) {
                String[] res = line.split(":");
                if (!dependencies.keySet().contains(res[0])) {
                    dependencies.put(res[0], new HashSet<String>());
                } 
                if (!dependencies.keySet().contains(res[1])) {
                    dependencies.put(res[1], new HashSet<String>());
                } 
                dependencies.get(res[0]).add(res[1]);
            }
        }
        br.close();

        // create a src.main.Node object for each query
        this.nodes = new HashMap<String, Node>();
        for (String query: dependencies.keySet()) {
            this.nodes.put(query, new Node(query));
        }

        // for each query, correctly set neighbors and set root
        for (String query: dependencies.keySet()) {
            HashSet<Node> neighbors = new HashSet<Node>();
            for (String s2: dependencies.get(query)) {
                neighbors.add(this.nodes.get(s2));
            }  
            this.nodes.get(query).setNeighbors(neighbors);
        }
        this.root = this.nodes.get(root);
    }

    /**
     * 
     * @return list of nodes for a random walk from root
     */
    public ArrayList<Node> randomWalk() {
        return randomWalk(this.root);
    }

    /**
     * 
     * @param start
     * @return list of nodes for a random walk from start node
     */
    public ArrayList<Node> randomWalk(Node start) {
        ArrayList<Node> walk = new ArrayList<Node>();
        Node current = start;
        // System.out.println(current.getNeighborList());
        while (current.hasNextNode()) {
            walk.add(current);
            current = current.nextNode();
        }
        walk.add(current);
        return walk;
    }

    /**
     * 
     * @return map of strings to nodes
     */
    public HashMap<String, Node> getNodes() {
        return this.nodes;
    }

}