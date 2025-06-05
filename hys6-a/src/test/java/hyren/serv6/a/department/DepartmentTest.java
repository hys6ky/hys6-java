package hyren.serv6.a.department;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.a.AppMain;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest(classes = AppMain.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@RunWith(SpringRunner.class)
public class DepartmentTest {

    @LocalServerPort
    private int port;

    private URL base;

    @Autowired
    private TestRestTemplate testTemplate;

    static Map<String, Object> HyrenAuthorization = new HashMap<>();

    @BeforeClass
    public static void login() {
        try {
            URL url = new URL("http://localhost:20001/hyren-gateway/login?user_id=1000&password=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
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
            Map<String, Object> jsonObject = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Map<String, Object> jsonData = (Map<String, Object>) jsonObject.get("data");
            HyrenAuthorization.put("tokenHeaderKeyName", jsonData.get("tokenHeaderKeyName"));
            HyrenAuthorization.put("accessToken", jsonData.get("accessToken"));
            System.out.println("Response content: " + response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Before
    public void setUp() throws Exception {
        String url = String.format("http://localhost:%d/A/department/", port);
        System.out.println(String.format("port is : [%d]", port));
        this.base = new URL(url);
    }

    @Test
    public void testAddDepartmentInfo() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testTemplate.postForObject(this.base.toString() + "addDepartmentInfo?dep_name=三磊&dep_remark=''", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testDeleteDepartmentInfo() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testTemplate.postForObject(this.base.toString() + "deleteDepartmentInfo?dep_id=-123", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testUpdateDepartmentInfo() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testTemplate.postForObject(this.base.toString() + "updateDepartmentInfo?dep_id=-123&dep_name=FFF", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void getDepartmentInfo() {
        try {
            URL url = new URL("http://localhost:18000/A/department/getDepartmentInfo?currPage=1&pageSize=10");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
            String token = (String) HyrenAuthorization.get("accessToken");
            connection.setRequestProperty(tokenName, token);
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println("Response content: " + response.toString());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
