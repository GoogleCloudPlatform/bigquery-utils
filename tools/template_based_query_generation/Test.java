public class Test {
    
    public static void main(String[] args) throws Exception {
        MarkovChain m = new MarkovChain("dependencies.txt");
        m.sampleWalk(50);
    }
}