package hyren.serv6.a.login;

import hyren.serv6.a.AppMain;
import junit.framework.TestCase;
import org.junit.Before;
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
import java.net.URL;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = AppMain.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LoginControllerTest extends TestCase {

    @LocalServerPort
    private int port;

    private URL base;

    @Autowired
    private TestRestTemplate testTemplate;

    @Before
    public void setUp() throws Exception {
        String url = String.format("http://localhost:%d/A/login/", port);
        System.out.println(url.toString());
        this.base = new URL(url);
    }

    @Test
    public void testGetHyrenHost() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testTemplate.postForObject(this.base.toString() + "getHyrenHost", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testGetSysName() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testTemplate.postForObject(this.base.toString() + "getSysName", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testGetWaterMark() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testTemplate.postForObject(this.base.toString() + "getWaterMark", formEntity, String.class);
        System.out.println(response);
    }
}
