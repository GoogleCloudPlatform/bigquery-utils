public class LoopTest {
    public String test() {
        String cities = {"Austin", "Beijing", "Cairo"};
        String sql = "SELECT * FROM customer WHERE ";
        boolean includeOr = false;
        for (String city : cities) {
            if (includeOr) {
                sql += " OR ";
            } else {
                includeOr = true;
            }
            sql += "city = " + city;
        }
        return sql;
    }
}