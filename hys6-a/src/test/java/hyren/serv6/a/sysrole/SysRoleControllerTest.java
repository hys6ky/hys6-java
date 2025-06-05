package hyren.serv6.a.sysrole;

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
public class SysRoleControllerTest extends TestCase {

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
        String url = String.format("http://localhost:%d/A/sysrole/", port);
        this.base = new URL(url);
    }

    @Test
    public void testGetUserFunctionMenu() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        Map<String, Object> requestBody = new HashMap<String, Object>();
        requestBody.put("userIsAdmin", 02);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(requestBody, headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "getUserFunctionMenu?userIsAdmin=02", request, String.class);
        String responseBody = responseEntity.getBody();
        Map<String, Object> jsonObject = JsonUtil.toObject(responseBody, new TypeReference<Map<String, Object>>() {
        });
        System.out.println(responseBody);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertEquals("Success", jsonObject.get("message"));
    }

    @Test
    public void testGetUserRole() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "getUserRole", request, String.class);
        String responseBody = responseEntity.getBody();
        System.out.println(responseBody);
        Map<String, Object> jsonObject = JsonUtil.toObject(responseBody, new TypeReference<Map<String, Object>>() {
        });
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertEquals("Success", jsonObject.get("message"));
    }

    @Test
    public void testGetSysRoleInfo() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "getSysRoleInfo?currPage=1&pageSize=10", request, String.class);
        String responseBody = responseEntity.getBody();
        System.out.println(responseBody);
        Map<String, Object> jsonObject = JsonUtil.toObject(responseBody, new TypeReference<Map<String, Object>>() {
        });
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertEquals("Success", jsonObject.get("message"));
    }

    @Test
    public void testSaveSysRole() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "saveSysRole?role_id=&is_admin=01&" + "role_name=111&role_remark=test&role_menu=201&role_menu=202&role_menu=203", request, String.class);
        String responseBody = responseEntity.getBody();
        System.out.println(responseBody);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    }

    @Test
    public void testGetRoleInfo() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "getRoleInfo?role_id=01", request, String.class);
        String responseBody = responseEntity.getBody();
        System.out.println(responseBody);
        Map<String, Object> jsonObject = JsonUtil.toObject(responseBody, new TypeReference<Map<String, Object>>() {
        });
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
        assertEquals("Success", jsonObject.get("message"));
    }

    @Test
    public void testUpdateSysRole() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "updateSysRole?" + "role_id=-123&is_admin=02&role_name=111&role_remark=test&" + "role_menu=102&role_menu=2701&role_menu=2702&role_menu=2703&" + "role_menu=2704&role_menu=103", request, String.class);
        String responseBody = responseEntity.getBody();
        System.out.println(responseBody);
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    }

    @Test
    public void testDeleteSysRole() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        String tokenName = (String) HyrenAuthorization.get("tokenHeaderKeyName");
        String token = (String) HyrenAuthorization.get("accessToken");
        headers.add(tokenName, token);
        HttpEntity<Map<String, Object>> request = new HttpEntity<>(headers);
        ResponseEntity<String> responseEntity = testRestTemplate.postForEntity(this.base.toString() + "deleteSysRole?role_id=-123", request, String.class);
        String responseBody = responseEntity.getBody();
        System.out.println(responseBody);
        Map<String, Object> jsonObject = JsonUtil.toObject(responseBody, new TypeReference<Map<String, Object>>() {
        });
        assertEquals(HttpStatus.OK, responseEntity.getStatusCode());
    }
}
