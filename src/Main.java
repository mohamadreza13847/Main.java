import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.Scanner;
import org.json.JSONObject;



public class Main {
    private static final String OLLAMA_API_URL = "http://localhost:11434/api/generate";
    private static final String MODEL_NAME = "llama3.2:1b";
    private static final String DB_URL = "jdbc:sqlite:movies.db";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Choose search method: (database, llama)");
        String method = scanner.nextLine().trim().toLowerCase();

        System.out.println("Choose search type: (movie, director, genre, year, rating, actor, language, country, budget, boxoffice)");
        String searchType = scanner.nextLine().trim().toLowerCase();

        System.out.println("Enter search value:");
        String searchValue = scanner.nextLine().trim();

        if (method.equals("database")) {
            searchFromDatabase(searchType, searchValue);
        } else if (method.equals("llama")) {
            try {
                String result = queryOllama(searchType, searchValue);
                System.out.println("\nSearch Results:\n" + result);
            } catch (IOException e) {
                System.err.println("Error fetching movie recommendation: " + e.getMessage());
            }
        } else {
            System.out.println("Invalid search method.");
        }

        scanner.close();
    }

    public static void searchFromDatabase(String queryType, String queryValue) {
        try (Connection conn = DriverManager.getConnection(DB_URL)) {
            switch (queryType) {
                case "movie":
                    searchByMovie(conn, queryValue);
                    break;
                case "director":
                    searchByDirector(conn, queryValue);
                    break;
                case "genre":
                    searchByGenre(conn, queryValue);
                    break;
                case "year":
                    searchByYear(conn, queryValue);
                    break;
                case "rating":
                    searchByRating(conn, queryValue);
                    break;
                case "actor":
                    searchByActor(conn, queryValue);
                    break;
                case "language":
                    searchByLanguage(conn, queryValue);
                    break;
                case "country":
                    searchByCountry(conn, queryValue);
                    break;
                case "budget":
                    searchByBudget(conn, queryValue);
                    break;
                case "boxoffice":
                    searchByBoxOffice(conn, queryValue);
                    break;
                default:
                    System.out.println("Invalid search type.");
            }
        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }

    private static void searchByMovie(Connection conn, String title) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE title LIKE ?", "%" + title + "%");
    }

    private static void searchByDirector(Connection conn, String director) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE director LIKE ?", "%" + director + "%");
    }

    private static void searchByGenre(Connection conn, String genre) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE genre LIKE ?", "%" + genre + "%");
    }

    private static void searchByYear(Connection conn, String year) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE year = ?", year);
    }

    private static void searchByRating(Connection conn, String rating) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE IMDbRating >= ?", rating);
    }

    private static void searchByActor(Connection conn, String actor) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE actors LIKE ?", "%" + actor + "%");
    }

    private static void searchByLanguage(Connection conn, String language) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE language LIKE ?", "%" + language + "%");
    }

    private static void searchByCountry(Connection conn, String country) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE country LIKE ?", "%" + country + "%");
    }

    private static void searchByBudget(Connection conn, String budget) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE budget >= ?", budget);
    }

    private static void searchByBoxOffice(Connection conn, String boxOffice) throws SQLException {
        executeQuery(conn, "SELECT * FROM movies WHERE boxoffice >= ?", boxOffice);
    }

    private static void executeQuery(Connection conn, String sql, String param) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, param);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                System.out.println("🎬 Title: " + rs.getString("title"));
                System.out.println("⭐ IMDb Rating: " + rs.getDouble("IMDbRating"));
                System.out.println("🎭 Actor: " + rs.getString("actors"));
                System.out.println("🎬 Director: " + rs.getString("director"));
                System.out.println("🌍 Country: " + rs.getString("country"));
                System.out.println("💰 Box Office: $" + rs.getDouble("boxoffice") + "M");
                System.out.println("-------------------------");
            }
        }
    }

    public static String queryOllama(String queryType, String queryValue) throws IOException {
        URL url = new URL(OLLAMA_API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String prompt = buildPrompt(queryType, queryValue);
        String jsonInputString = "{ \"model\": \"" + MODEL_NAME + "\", \"prompt\": \"" + prompt + "\" }";

        try (OutputStream os = conn.getOutputStream()) {
            byte[] input = jsonInputString.getBytes("utf-8");
            os.write(input, 0, input.length);
        }

        BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"));
        StringBuilder fullResponse = new StringBuilder();
        String responseLine;

        while ((responseLine = br.readLine()) != null) {
            try {
                JSONObject json = new JSONObject(responseLine.trim());
                if (json.has("response")) {
                    fullResponse.append(json.getString("response"));
                }
            } catch (Exception e) {
                continue;
            }
        }

        return fullResponse.toString().trim();
    }

    private static String buildPrompt(String queryType, String queryValue) {
        switch (queryType) {
            case "movie":
                return "Find movies related to: " + queryValue;
            case "actor":
                return "List movies starring: " + queryValue;
            case "director":
                return "List movies directed by: " + queryValue;
            case "genre":
                return "List movies in the genre: " + queryValue;
            case "rating":
                return "Find movies with a rating of at least: " + queryValue;
            case "language":
                return "Find movies available in language: " + queryValue;
            case "country":
                return "Find movies produced in: " + queryValue;
            case "budget":
                return "Find movies with a budget over: " + queryValue;
            case "boxoffice":
                return "Find movies with a box office over: " + queryValue;
            default:
                return "Invalid search type";
        }
    }
}

