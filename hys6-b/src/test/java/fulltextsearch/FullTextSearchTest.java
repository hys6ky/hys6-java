package fulltextsearch;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.JsonUtil;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class FullTextSearchTest {

    @Test
    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    public void getTableColumnInfoBySql() {
        try {
            URL url = new URL("http://127.0.0.1:18000/B/fulltextsearch/getCollectFiles?queryNum=" + 2);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Method(desc = "", logicStep = "")
    @Param(name = "fullTextSearchMethod", desc = "", range = "", valueIfNull = "unstructuredFileSearch")
    @Param(name = "queryKeyword", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    public void fullTextSearch() {
        try {
            URL url = new URL("http://127.0.0.1:18000/B/fulltextsearch/fullTextSearch?fullTextSearchMethod=fullTextSearch&queryKeyword=good&currPage=1&pageSize=5");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Method(desc = "", logicStep = "")
    @Param(name = "imageAddress", desc = "", range = "", nullable = true)
    public void searchByMap() {
        try {
            String path = "D:/ceshipic.jpg";
            URL url = new URL("http://127.0.0.1:18000/B/fulltextsearch/searchByMap?imageAddress=" + path);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
