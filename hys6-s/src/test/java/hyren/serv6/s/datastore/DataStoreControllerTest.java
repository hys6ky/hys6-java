package hyren.serv6.s.datastore;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@RunWith(SpringRunner.class)
@SpringBootTest()
public class DataStoreControllerTest {

    @Value("${server.port}")
    private int port;

    @Test
    public void addDataStore() {
        try {
            URL url = new URL("http://localhost:" + port + "/A//datastore/addDataStore");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setDoOutput(true);
            StringBuilder requestBody = new StringBuilder();
            requestBody.append("dsl_name=").append(URLEncoder.encode("ceshi", "UTF-8"));
            requestBody.append("&store_type=").append(1);
            requestBody.append("&is_hadoopclient=").append(0);
            requestBody.append("&dsl_remark=").append(URLEncoder.encode("1", "UTF-8"));
            requestBody.append("&dslad_remark=").append(URLEncoder.encode("1", "UTF-8"));
            requestBody.append("&dataStoreLayerAttr=").append(URLEncoder.encode("[{\"storage_property_key\":\"database_type\",\"is_file\":0,\"storage_property_val\":\"MYSQL\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"database_driver\",\"is_file\":0,\"storage_property_val\":\"com.mysql.jdbc.Driver\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"user_name\",\"is_file\":0,\"storage_property_val\":\"111\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"database_pwd\",\"is_file\":0,\"storage_property_val\":\"111\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"database_name\",\"is_file\":0,\"storage_property_val\":\"111\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"jdbc_url\",\"is_file\":0,\"storage_property_val\":\"jdbc:mysql://ip:port/111?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull\",\"dsla_remark\":\"11\"}]", "UTF-8"));
            requestBody.append("&files").append(URLEncoder.encode("", "UTF-8"));
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(requestBody.toString().getBytes(StandardCharsets.UTF_8));
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } else {
                System.out.println("Error response code: " + responseCode);
            }
            connection.disconnect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void updateDataStore() {
        try {
            URL url = new URL("http://localhost:" + port + "/A//datastore/updateDataStore");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded");
            connection.setDoOutput(true);
            StringBuilder requestBody = new StringBuilder();
            requestBody.append("dsl_name=").append(URLEncoder.encode("ceshi", "UTF-8"));
            requestBody.append("&store_type=").append(1);
            requestBody.append("&is_hadoopclient=").append(0);
            requestBody.append("&dsl_remark=").append(URLEncoder.encode("1", "UTF-8"));
            requestBody.append("&dslad_remark=").append(URLEncoder.encode("1", "UTF-8"));
            requestBody.append("&dsl_id=").append(1113512002208137216L);
            requestBody.append("&dataStoreLayerAttr=").append(URLEncoder.encode("[{\"storage_property_key\":\"database_type\",\"is_file\":0,\"storage_property_val\":\"MYSQL\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"database_driver\",\"is_file\":0,\"storage_property_val\":\"com.mysql.jdbc.Driver\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"user_name\",\"is_file\":0,\"storage_property_val\":\"111\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"database_pwd\",\"is_file\":0,\"storage_property_val\":\"111\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"database_name\",\"is_file\":0,\"storage_property_val\":\"111\",\"dsla_remark\":\"1\"},{\"storage_property_key\":\"jdbc_url\",\"is_file\":0,\"storage_property_val\":\"jdbc:mysql://ip:port/111?characterEncoding=utf8&zeroDateTimeBehavior=convertToNull\",\"dsla_remark\":\"11\"}]", "UTF-8"));
            requestBody.append("&files").append(URLEncoder.encode("", "UTF-8"));
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(requestBody.toString().getBytes(StandardCharsets.UTF_8));
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                try (BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
                    String line;
                    while ((line = in.readLine()) != null) {
                        System.out.println(line);
                    }
                }
            } else {
                System.out.println("Error response code: " + responseCode);
            }
            connection.disconnect();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void deleteDataStore() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/deleteDataStore?dsl_id=1113512002208137216");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void searchDataStore() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/searchDataStore");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void searchDataStoreById() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/searchDataStoreById?dsl_id=918513912742150144");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void searchDBName() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/searchDBName");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void searchContrastTypeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/searchContrastTypeInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getDataTypeMsg() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/getDataTypeMsg?db_name1=POSTGRESQL&db_name2=MYSQL");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void generateExcel() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/generateExcel?fileName=data_type_contrast.xlsx");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void downloadFile() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/downloadFile?fileName=data_type_contrast.xlsx");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getLayerAttrByIdAndType() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/getLayerAttrByIdAndType?dsl_id=1111616797460660224&store_type=1&is_hadoopclient=0");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void downloadConfFile() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/downloadConfFile?fileName=data_type_contrast.xlsx&filePath=D:/uploadfiles");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getDataLayerAttrKey() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/getDataLayerAttrKey?store_type=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getExternalTableAttrKey() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/getExternalTableAttrKey?store_type=1&is_hadoopclient=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAttrKeyByDatabaseType() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/getAttrKeyByDatabaseType?store_type=1&is_hadoopclient=1&db_name=MYSQL");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getDBConnectionMsg() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/datastore/getDBConnectionMsg?db_name=DB2_V1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
