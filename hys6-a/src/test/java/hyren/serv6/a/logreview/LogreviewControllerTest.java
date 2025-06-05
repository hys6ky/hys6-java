package hyren.serv6.a.logreview;

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

@SpringBootTest(classes = AppMain.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@RunWith(SpringRunner.class)
public class LogreviewControllerTest extends TestCase {

    @LocalServerPort
    private int port;

    private URL base;

    @Autowired
    private TestRestTemplate testRestTemplate;

    @Before
    public void setUp() throws Exception {
        String url = String.format("http://localhost:%d/A/logreview/", port);
        System.out.println(url);
        this.base = new URL(url);
    }

    @Test
    public void testSearchSystemLogByPage() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "searchSystemLogByPage?currPage=1&pageSize=10", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testSearchSystemLogByIdOrDate() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "searchSystemLogByIdOrDate?request_date=20230526&user_id=1000&currPage=1&pageSize=10", formEntity, String.class);
        System.out.println(response);
    }

    @Test
    public void testDownloadSystemLog() {
        HttpHeaders headers = new HttpHeaders();
        MediaType type = MediaType.parseMediaType("application/json; charset=UTF-8");
        headers.setContentType(type);
        headers.add("Accept", MediaType.APPLICATION_JSON.toString());
        HttpEntity<String> formEntity = new HttpEntity<String>(headers);
        String response = this.testRestTemplate.postForObject(this.base.toString() + "downloadSystemLog?request_date=20230526&user_id=1000&currPage=1&pageSize=10", formEntity, String.class);
        System.out.println(response);
    }
}
