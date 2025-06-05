package agent.dbagentconf.tableconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Map;

public class TableconfTest {

    @Test
    public void getInitInfo() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getInitInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976418251909107712";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTableDataCount() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getTableDataCount");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String sql = "select * from agent_info";
            String params = "colSetId=976418251909107712&pageSql=11";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTableCycle() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getTableCycle");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "table_id=976418251909107712";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTableInfo() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getTableInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "table_id=976418251909107712";
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "193");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getPageSQL() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getPageSQL");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "tableId=936293618669326336";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getSqlColumnData() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getSqlColumnData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976483279622377472&tableId=1090604646671585283";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "95");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getColumnInfo() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getColumnInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976483279622377472&tableId=1090604646671585283";
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "193");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAllSQLs() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getAllSQLs");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976483279622377472";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getSingleTableSQL() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getSingleTableSQL");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=936293604870066176&tableName=warehouse2";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getSQLInfoByColSetId() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getSQLInfoByColSetId");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=936293604870066176";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getColumnInfoByColSetId() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getColumnInfoByColSetId");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=936293604870066176";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getTableSetUnloadData() {
        try {
            URL url = new URL("http://localhost:20003/B/tableconf/getTableSetUnloadData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "table_id=936293618669326336";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
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
