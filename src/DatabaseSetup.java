import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseSetup {
    public static void main(String[] args) {
        String url = "jdbc:sqlite:movies.db"; // فایل دیتابیس SQLite

        String createTableSQL = "CREATE TABLE IF NOT EXISTS movies ("
                + "id INTEGER PRIMARY KEY AUTOINCREMENT, "
                + "title TEXT NOT NULL, "
                + "year INTEGER, "
                + "genre TEXT, "
                + "director TEXT, "
                + "rating REAL);";

        try (Connection conn = DriverManager.getConnection(url);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
            System.out.println("Table 'movies' created successfully.");
        } catch (SQLException e) {
            System.out.println("Database error: " + e.getMessage());
        }
    }
}

