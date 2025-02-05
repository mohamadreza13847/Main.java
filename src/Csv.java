import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Csv {
    private static final String DB_URL = "jdbc:sqlite:movies.db";
    private static final String CSV_FILE = "movies.csv";

    public static void main(String[] args) {
        String insertSQL = "INSERT OR IGNORE INTO movies (ID, Title, Year, Genre, Director, Rating) VALUES (?, ?, ?, ?, ?, ?)";

        try (Connection conn = DriverManager.getConnection(DB_URL);
             BufferedReader br = new BufferedReader(new FileReader(CSV_FILE));
             PreparedStatement pstmt = conn.prepareStatement(insertSQL)) {

            conn.setAutoCommit(false);

            String line = br.readLine(); // رد کردن هدر
            while ((line = br.readLine()) != null) {
                String[] values = line.split(",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)");

                if (values.length == 6) {
                    try {
                        pstmt.setInt(1, Integer.parseInt(values[0].trim()));  // ID
                        pstmt.setString(2, values[1].trim());  // Title
                        pstmt.setInt(3, Integer.parseInt(values[2].trim()));  // Year
                        pstmt.setString(4, values[3].trim());  // Genre
                        pstmt.setString(5, values[4].trim());  // Director
                        pstmt.setDouble(6, Double.parseDouble(values[5].trim()));  // Rating
                        pstmt.addBatch();
                    } catch (NumberFormatException e) {
                        System.out.println("Skipping invalid row: " + line);
                    }
                }
            }

            pstmt.executeBatch();
            conn.commit();
            System.out.println("Data successfully inserted into the database.");

        } catch (IOException | SQLException e) {
            e.printStackTrace();
        }
    }
}
