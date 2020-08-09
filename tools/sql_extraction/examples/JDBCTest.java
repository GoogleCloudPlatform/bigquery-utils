import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class JDBCTest {
    private static final String URL = "jdbc:mysql://localhost/db";

    public static void main(String[] args) {
        Connection conn = DriverManager.getConnection(URL, "root", "root");
        Statement stmt = conn.createStatement();

        String sql = "SELECT * FROM customer";
        ResultSet rs = stmt.executeQuery(sql);
    }
}