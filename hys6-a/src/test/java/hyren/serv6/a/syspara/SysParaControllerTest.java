package hyren.serv6.a.syspara;

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
import org.springframework.http.*;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;

@SpringBootTest(classes = AppMain.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@RunWith(SpringRunner.class)
public class SysParaControllerTest extends TestCase {

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
        String url = String.format("http://localhost:%d/A/syspara/", port);
        this.base = new URL(url);
    }

    @Test
    public void testGetSysPara() {
        ResponseEntity<String> responseEntity = testRestTemplate.getForEntity(this.base.toString() + "getSysPara?currPage=1&pageSize=10", String.class);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    }

    @Test
    public void testDeleteSysPara() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "deleteSysPara?para_id=1111690754519990272&para_name=测试111", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testAddSysPara() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "addSysPara?para_name=111&para_value=test111&para_type=111", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testUpdateSysPara() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "updateSysPara?para_id=1111&para_name=111&para_value=test222", formEntity, String.class);
        System.out.println(response);
    }
}
