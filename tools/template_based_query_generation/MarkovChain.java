import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.io.BufferedReader;
import java.io.FileReader;

public class MarkovChain {
    
    private HashMap<String, Node> nodes;
    private Node root;

    // creates nodes and dependencies from file with dependencies
    public MarkovChain(String filename) throws Exception {
        HashMap<String, HashSet<String>> edges = new HashMap<String, HashSet<String>>();
        BufferedReader br = new BufferedReader(new FileReader(filename));
        String line;
        while ((line = br.readLine()) != null) {
            if (line.length() > 2 && 
                line.charAt(0) != ' ' && 
                !(line.charAt(0) == '/' && 
                line.charAt(1) == '/')) {
                String[] res = line.split(":");
                if (edges.keySet().contains(res[0])) {
                    edges.get(res[0]).add(res[1]);
                } else {
                    edges.put(res[0], new HashSet<String>());
                    edges.get(res[0]).add(res[1]);
                }
            }
        }
        br.close();
        HashSet<String> distinctNodes = new HashSet<String>();
        for (String s1: edges.keySet()) {
            distinctNodes.add(s1);
            for (String s2: edges.get(s1)) {
                distinctNodes.add(s2);
            }
        }
        this.nodes = new HashMap<String, Node>();
        for (String s: distinctNodes) {
            this.nodes.put(s, new Node(s));
        }
        for (String s1: distinctNodes) {
            HashSet<Node> neighbors = new HashSet<Node>();
            if (edges.keySet().contains(s1)) {
                for (String s2: edges.get(s1)) {
                    neighbors.add(this.nodes.get(s2));
                }
            }  
            this.nodes.get(s1).setNeighbors(neighbors);
        }
        this.root = this.nodes.get("queryroot");
    }

    public HashMap<String, Node> getNodes() {
        return this.nodes;
    }

    public void setNodes(HashMap<String, Node> nodes) {
        this.nodes = nodes;
    }

    public ArrayList<Node> randomWalk() {
        return randomWalk(this.root);
    }

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

    public void sampleWalk(int num) {
        sampleWalk(num, this.root);
    }

    public void sampleWalk(int num, Node start) {
        for (int i = 0; i < num; i++) {
            ArrayList<Node> walk = this.randomWalk(start);
            for (Node n: walk) {
                System.out.print(n.toString().toUpperCase() + " ");
            }
            System.out.println();
        }
    }
}