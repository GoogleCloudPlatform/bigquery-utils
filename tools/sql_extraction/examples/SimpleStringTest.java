public class SimpleStringTest {
    public String test() {
        System.out.println("This string is not an SQL query");
        String sql = "SELECT * FROM customer";
        return sql;
    }
}