/**
 * Main class to test functionality of classes
 */
public class Test {
    
    public static void main(String[] args) throws Exception {
        MarkovChain m = new MarkovChain("dialect_config/dependencies.txt", "queryroot");
        for (int i = 0; i < 10; i++){
            System.out.println(m.randomWalk());
        }
    }
}