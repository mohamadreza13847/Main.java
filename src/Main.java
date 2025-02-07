import java.io.*;
import java.net.HttpURLConnection;
import java.net.URL;
import java.sql.*;
import java.util.*;
import org.json.JSONObject;

public class Main {
    private static final String OLLAMA_API_URL = "http://localhost:11434/api/generate";
    private static final String MODEL_NAME = "llama3.2:1b";
    private static final String DB_URL = "jdbc:sqlite:movies.db";
    private static final String RAG_API_URL = "http://localhost:5000/query";
    private static final String EXTERNAL_API_KEY = "5b8404295a2e6350004505178ff7670c";// آدرس RAG
    private static final String EXTERNAL_API_URL = "https://api.themoviedb.org/3/search/movie";

    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);

        System.out.println("Choose search method: (database, llama, rag, external)");
        String method = scanner.nextLine().trim().toLowerCase();

        System.out.println("Choose search type: (movie, director, genre, rating, actor)");
        String searchType = scanner.nextLine().trim().toLowerCase();

        System.out.println("Enter search value:");
        String searchValue = scanner.nextLine().trim();

        switch (method) {
            case "database":
                searchFromDatabase(searchType, searchValue);
                break;
            case "llama":
                try {
                    String result = queryOllama(searchType, searchValue);
                    System.out.println("\nSearch Results:\n" + result);
                } catch (IOException e) {
                    System.err.println("Error fetching movie recommendation: " + e.getMessage());
                }
                break;
            case "rag":
                try {
                    String ragResult = queryRAG(searchType, searchValue);
                    System.out.println("\nRAG Search Results:\n" + ragResult);
                } catch (IOException e) {
                    System.err.println("Error with RAG search: " + e.getMessage());
                    System.out.println("Falling back to database search...");
                    searchFromDatabase(searchType, searchValue);
                }
                break;
            case "external":
                try {
                    String externalData = fetchExternalData(searchValue);
                    String result = queryOllamaWithExternalData(searchValue, externalData);
                    System.out.println("\nResults Based on External API:\n" + result);
                } catch (IOException e) {
                    System.err.println("Error fetching external data: " + e.getMessage());
                }
                break;
            default:
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
                case "rating":
                    searchByRating(conn, queryValue);
                    break;
                case "actor":
                    searchByActor(conn, queryValue);
                    break;
                default:
                    System.out.println("Invalid search type.");
            }
        } catch (SQLException e) {
            System.err.println("Database connection error: " + e.getMessage());
        }
    }

    private static void searchByMovie(Connection conn, String title) throws SQLException {
        String sql = "SELECT tb.primaryTitle, tr.averageRating, nb.primaryName, tp.category " +
                "FROM title_basics tb " +
                "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst " +
                "LEFT JOIN title_principals tp ON tb.tconst = tp.tconst " +
                "LEFT JOIN name_basics nb ON tp.nconst = nb.nconst " +
                "WHERE tb.primaryTitle LIKE ? AND tb.titleType = 'movie'";
        executeQuery(conn, sql, "%" + title + "%");
    }

    private static void searchByDirector(Connection conn, String director) throws SQLException {
        String sql = "SELECT tb.primaryTitle, tr.averageRating, nb.primaryName, tp.category " +
                "FROM title_basics tb " +
                "JOIN title_principals tp ON tb.tconst = tp.tconst " +
                "JOIN name_basics nb ON tp.nconst = nb.nconst " +
                "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst " +
                "WHERE tp.category = 'director' AND nb.primaryName LIKE ? AND tb.titleType = 'movie'";
        executeQuery(conn, sql, "%" + director + "%");
    }

    private static void searchByGenre(Connection conn, String genre) throws SQLException {
        String sql = "SELECT tb.primaryTitle, tr.averageRating, nb.primaryName, tp.category " +
                "FROM title_basics tb " +
                "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst " +
                "LEFT JOIN title_principals tp ON tb.tconst = tp.tconst " +
                "LEFT JOIN name_basics nb ON tp.nconst = nb.nconst " +
                "WHERE tb.genres LIKE ? AND tb.titleType = 'movie'";
        executeQuery(conn, sql, "%" + genre + "%");
    }

    private static void searchByRating(Connection conn, String rating) throws SQLException {
        String sql = "SELECT tb.primaryTitle, tr.averageRating, nb.primaryName, tp.category " +
                "FROM title_basics tb " +
                "JOIN title_ratings tr ON tb.tconst = tr.tconst " +
                "LEFT JOIN title_principals tp ON tb.tconst = tp.tconst " +
                "LEFT JOIN name_basics nb ON tp.nconst = nb.nconst " +
                "WHERE tr.averageRating >= ? AND tb.titleType = 'movie'";
        executeQuery(conn, sql, rating);
    }

    private static void searchByActor(Connection conn, String actor) throws SQLException {
        String sql = "SELECT tb.primaryTitle, tr.averageRating, nb.primaryName, tp.category " +
                "FROM title_basics tb " +
                "JOIN title_principals tp ON tb.tconst = tp.tconst " +
                "JOIN name_basics nb ON tp.nconst = nb.nconst " +
                "LEFT JOIN title_ratings tr ON tb.tconst = tr.tconst " +
                "WHERE tp.category = 'actor' AND nb.primaryName LIKE ? AND tb.titleType = 'movie'";
        executeQuery(conn, sql, "%" + actor + "%");
    }

    private static void executeQuery(Connection conn, String sql, String param) throws SQLException {
        Map<String, Set<String>> movieCast = new LinkedHashMap<>();
        Map<String, Double> movieRatings = new HashMap<>();

        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, param);
            ResultSet rs = pstmt.executeQuery();

            while (rs.next()) {
                String title = rs.getString("primaryTitle");
                double rating = rs.getDouble("averageRating");
                String name = rs.getString("primaryName");
                String category = rs.getString("category");

                movieRatings.putIfAbsent(title, rating);
                if (name != null && category != null) {
                    movieCast.computeIfAbsent(title, k -> new LinkedHashSet<>()).add(name + " (" + category + ")");
                }
            }

            for (Map.Entry<String, Set<String>> entry : movieCast.entrySet()) {
                System.out.println("🎬 Title: " + entry.getKey());
                System.out.println("⭐ IMDb Rating: " + movieRatings.getOrDefault(entry.getKey(), 0.0));
                System.out.println("🎭 Cast & Crew:");
                entry.getValue().forEach(name -> System.out.println("- " + name));
                System.out.println("-------------------------");
            }
        }
    }

    public static String queryRAG(String queryType, String queryValue) throws IOException {
        URL url = new URL(RAG_API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        String jsonInputString = "{ \"queryType\": \"" + queryType + "\", \"queryValue\": \"" + queryValue + "\" }";

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
    public static String buildPrompt(String queryType, String queryValue) {
        String prompt;
        switch (queryType.toLowerCase()) {
            case "movie":
                prompt = "Provide detailed information about the movie titled: \"" + queryValue + "\". Include genre, rating, and key cast members.";
                break;
            case "director":
                prompt = "List all movies directed by \"" + queryValue + "\" along with their IMDb ratings and notable actors.";
                break;
            case "genre":
                prompt = "Recommend popular movies in the \"" + queryValue + "\" genre with brief descriptions and ratings.";
                break;
            case "rating":
                prompt = "Find movies with an IMDb rating of at least \"" + queryValue + "\". List the titles and their directors.";
                break;
            case "actor":
                prompt = "List movies starring \"" + queryValue + "\" along with their genres, ratings, and release years.";
                break;
            default:
                prompt = "Find information related to: " + queryValue;
        }
        return prompt;
    }
    public static String fetchExternalData(String query) throws IOException {
        String urlStr = EXTERNAL_API_URL + "?api_key=" + EXTERNAL_API_KEY + "&query=" + query.replace(" ", "%20");
        URL url = new URL(urlStr);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("GET");

        BufferedReader br = new BufferedReader(new InputStreamReader(conn.getInputStream()));
        StringBuilder response = new StringBuilder();
        String line;

        while ((line = br.readLine()) != null) {
            response.append(line);
        }
        br.close();

        return response.toString();
    }

    // 2️⃣ ارسال اطلاعات ترکیبی به Ollama
    public static String queryOllamaWithExternalData(String userQuery, String tmdbData) throws IOException {
        URL url = new URL(OLLAMA_API_URL);
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        conn.setRequestMethod("POST");
        conn.setRequestProperty("Content-Type", "application/json");
        conn.setDoOutput(true);

        // ساخت پرامپت ترکیبی
        String prompt = "User Query: " + userQuery + "\nTMDb Data: " + tmdbData;
        String jsonInputString = "{ \"model\": \"" + MODEL_NAME + "\", \"prompt\": \"" + prompt.replace("\"", "\\\"") + "\" }";

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
}


