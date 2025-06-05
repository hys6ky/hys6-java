package websqlquery;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

public class WebsqlqueryTest {

    @Test
    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    public void getTableInfoByTableName_cache() {
        try {
            URL url = new URL("http://127.0.0.1:18000/B/websqlquery/getTableInfoByTableName_cache?table_name=agent_info&begin=1&end=2");
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
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "", valueIfNull = "1")
    @Param(name = "end", desc = "", range = "", valueIfNull = "10")
    public void queryDataBasedOnTableName() {
        try {
            URL url = new URL("http://127.0.0.1:20003/B/websqlquery/queryDataBasedOnTableName?table_name=agent_info&begin=1&end=2");
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
    @Param(name = "querySQL", desc = "", range = "")
    @Param(name = "begin", desc = "", range = "", valueIfNull = "1")
    @Param(name = "end", desc = "", range = "", valueIfNull = "100")
    public void queryDataBasedOnSql() {
        String sql = "select * from agent_info";
        try {
            URL url = new URL("http://127.0.0.1:18000/B/websqlquery/queryDataBasedOnSql?querySQL=" + encodeUrl(sql) + "&begin=1&end=2");
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
    public void getWebSQLTreeData() {
        try {
            URL url = new URL("http://127.0.0.1:18000/B/websqlquery/getWebSQLTreeData");
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
    public void getAllTableNameByPlatform() {
        try {
            URL url = new URL("http://127.0.0.1:18000/B/websqlquery/getAllTableNameByPlatform");
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
    @Param(name = "table_name", desc = "", range = "")
    public void getColumnsByTableName() {
        try {
            URL url = new URL("http://127.0.0.1:18000/B/websqlquery/getColumnsByTableName?table_name=agent_info");
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
    @Param(name = "sql", desc = "", range = "")
    public void getTableColumnInfoBySql() {
        try {
            String sql = "select * from agent_info";
            URL url = new URL("http://127.0.0.1:18000/B/websqlquery/getTableColumnInfoBySql?sql=" + encodeUrl(sql));
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

    public static String encodeUrl(String url) {
        try {
            String encodedUrl = URLEncoder.encode(url, "UTF-8");
            return encodedUrl.replaceAll("\\+", "%20");
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }
}
