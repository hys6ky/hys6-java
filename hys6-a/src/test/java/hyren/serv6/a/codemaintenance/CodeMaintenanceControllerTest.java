package hyren.serv6.a.codemaintenance;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;

@RunWith(SpringRunner.class)
@SpringBootTest()
public class CodeMaintenanceControllerTest {

    @Value("${server.port}")
    private int port;

    @Before
    public void setUrl() {
    }

    @Test
    public void testGetCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/getCodeInfo");
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
    public void saveCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/saveCodeInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String jsonParam = "[{\"code_type_name\":\"3\",\"code_value\":\"3\",\"code_classify\":\"1\",\"code_classify_name\":\"1\",\"code_remark\":\"1\"}]";
            byte[] postData = jsonParam.getBytes(StandardCharsets.UTF_8);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
            try (OutputStream os = connection.getOutputStream()) {
                os.write(postData);
            }
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void updateCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/updateCodeInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String jsonParam = "[{\"code_type_name\":\"5\",\"code_value\":\"5\",\"code_classify\":\"1\",\"code_classify_name\":\"1\",\"code_remark\":\"1\"}]";
            byte[] postData = jsonParam.getBytes(StandardCharsets.UTF_8);
            connection.setDoOutput(true);
            connection.setRequestProperty("Content-Type", "application/json");
            connection.setRequestProperty("Content-Length", Integer.toString(postData.length));
            try (OutputStream os = connection.getOutputStream()) {
                os.write(postData);
            }
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/deleteCodeInfo?code_classify=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getOrigSysInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/getOrigSysInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addOrigSysInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/addOrigSysInfo?orig_sys_code=ceshi1&orig_sys_name=ceshi1&orig_sys_remark=ceshi1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getOrigCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/getOrigCodeInfo?orig_sys_code=K01");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getCodeInfoByCodeClassify() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/getCodeInfoByCodeClassify?code_classify=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addOrigCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/addOrigCodeInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "orig_sys_code=ceshi1&orig_code_infos=[{\"code_type_name\":\"nan\",\"code_value\":\"n\",\"code_classify\":\"升水\",\"code_classify_name\":\"点对点\",\"code_remark\":\"sdfdsf\",\"orig_value\":\"12\",\"orig_sys_code\":\"ceshi1\"},{\"code_type_name\":\"女\",\"code_value\":\"f\",\"code_classify\":\"升水\",\"code_classify_name\":\"点对点\",\"code_remark\":\"sdfdsf\",\"orig_value\":\"12\",\"orig_sys_code\":\"ceshi1\"}]";
            connection.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(params);
            writer.flush();
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void updateOrigCodeInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/updateOrigCodeInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "orig_sys_code=ceshi1&orig_code_infos=[{\"orig_value\":\"121\",\"orig_sys_code\":\"ceshi1\",\"code_remark\":\"sdfdsf\",\"code_type_name\":\"nan\",\"code_value\":\"n\",\"code_classify\":\"升水\",\"orig_id\":\"1113148894142791680\",\"code_classify_name\":\"点对点\"},{\"orig_value\":\"121\",\"orig_sys_code\":\"ceshi1\",\"code_remark\":\"sdfdsf\",\"code_type_name\":\"女\",\"code_value\":\"f\",\"code_classify\":\"升水\",\"orig_id\":\"1113148894692245504\",\"code_classify_name\":\"点对点\"}]";
            connection.setDoOutput(true);
            OutputStreamWriter writer = new OutputStreamWriter(connection.getOutputStream());
            writer.write(params);
            writer.flush();
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println(response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void deleteOrigCodeInfo() {
        try {
            String code_classify = URLEncoder.encode("升水", "UTF-8");
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/deleteOrigCodeInfo?code_classify=" + code_classify + "&orig_sys_code=ceshi1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getAllCodeClassify() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/getAllCodeClassify");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void getOrigCodeInfoByCode() {
        try {
            String str = "1";
            URL url = new URL("http://localhost:" + port + "/A/codemaintenance/getOrigCodeInfoByCode?orig_sys_code=K01&code_classify=" + str);
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
