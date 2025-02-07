import java.io.*;
import java.sql.*;

public class DatabaseSetup {
    private static final String DB_URL = "jdbc:sqlite:movies.db";

    public static void main(String[] args) {
        createTables();
        importTSV("title.basics.tsv", "INSERT OR IGNORE INTO title_basics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", 9);
        importTSV("title.ratings.tsv", "INSERT OR IGNORE INTO title_ratings VALUES (?, ?, ?)", 3);
        importTSV("name.basics.tsv", "INSERT OR IGNORE INTO name_basics VALUES (?, ?, ?, ?, ?, ?)", 6);
        importTSV("title.principals.tsv", "INSERT OR IGNORE INTO title_principals VALUES (?, ?, ?, ?, ?, ?)", 6);
    }

    private static void createTables() {
        try (Connection conn = DriverManager.getConnection(DB_URL);
             Statement stmt = conn.createStatement()) {

            // جدول title_basics
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS title_basics (" +
                    "tconst TEXT PRIMARY KEY, " +
                    "titleType TEXT, " +
                    "primaryTitle TEXT, " +
                    "originalTitle TEXT, " +
                    "isAdult INTEGER, " +
                    "startYear INTEGER, " +
                    "endYear INTEGER, " +
                    "runtimeMinutes INTEGER, " +
                    "genres TEXT)");

            // جدول title_ratings
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS title_ratings (" +
                    "tconst TEXT PRIMARY KEY, " +
                    "averageRating REAL, " +
                    "numVotes INTEGER)");

            // جدول name_basics
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS name_basics (" +
                    "nconst TEXT PRIMARY KEY, " +
                    "primaryName TEXT, " +
                    "birthYear INTEGER, " +
                    "deathYear INTEGER, " +
                    "primaryProfession TEXT, " +
                    "knownForTitles TEXT)");

            // جدول title_principals
            stmt.executeUpdate("CREATE TABLE IF NOT EXISTS title_principals (" +
                    "tconst TEXT, " +
                    "ordering INTEGER, " +
                    "nconst TEXT, " +
                    "category TEXT, " +
                    "job TEXT, " +
                    "characters TEXT)");

            System.out.println("✅ Tables created successfully.");
        } catch (SQLException e) {
            System.err.println("❌ Error creating tables: " + e.getMessage());
        }
    }

    private static void importTSV(String fileName, String sql, int columnCount) {
        System.out.println("📥 Importing " + fileName);
        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            conn.setAutoCommit(false);
            try (BufferedReader reader = new BufferedReader(new FileReader(fileName));
                 PreparedStatement pstmt = conn.prepareStatement(sql)) {

                String line;
                int batchSize = 0;
                reader.readLine(); // رد کردن خط عنوان (Header)

                while ((line = reader.readLine()) != null) {
                    String[] data = line.split("\t", -1);

                    for (int i = 0; i < columnCount; i++) {
                        if (i < data.length && !data[i].equals("\\N")) {
                            pstmt.setString(i + 1, data[i]);
                        } else {
                            pstmt.setNull(i + 1, Types.NULL);
                        }
                    }

                    pstmt.addBatch();
                    batchSize++;

                    if (batchSize % 1000 == 0) { // هر 1000 رکورد را یکجا وارد کن
                        pstmt.executeBatch();
                        conn.commit();
                        batchSize = 0;
                    }
                }
                pstmt.executeBatch(); // وارد کردن باقی‌مانده داده‌ها
                conn.commit();
                System.out.println("✅ " + fileName + " imported successfully.");
            }
        } catch (Exception e) {
            System.err.println("❌ Error importing " + fileName + ": " + e.getMessage());
        }
    }
}