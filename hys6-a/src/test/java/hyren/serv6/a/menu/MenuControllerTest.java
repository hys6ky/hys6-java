package hyren.serv6.a.menu;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.a.AppMain;
import junit.framework.TestCase;
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
public class MenuControllerTest extends TestCase {

    @LocalServerPort
    private int port;

    private URL base;

    @Autowired
    private TestRestTemplate testRestTemplate;

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
        String url = String.format("http://localhost:%d/A/menu/", port);
        System.out.println(url);
        this.base = new URL(url);
    }

    @Test
    public void testGetMenu() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "getMenu", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testGetDefaultPage() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "getDefaultPage", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testGetTodayFeaturesInfo() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "getTodayFeaturesInfo", formEntity, String.class);
        System.out.println(response);
    }
}
