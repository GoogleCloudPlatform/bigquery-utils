public class ConcatTest {
    public String test() {
        String sql = "SELECT *\n" +
                " FROM customer\n" +
                " WHERE a = 1 AND b = 1";
        return sql;
    }
}