package hyren.serv6.a.codes;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

@RunWith(SpringRunner.class)
@SpringBootTest()
public class GenCodesItemControllerTest {

    @Value("${server.port}")
    private int port;

    @Test
    public void getValue() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codes/getValue?category=Store_type&&code=2");
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
    public void getCategoryItems() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codes/getCategoryItems?category=Store_type");
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
    public void getCodeItems() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codes/getCodeItems?category=Store_type");
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
    public void getAllCodeItems() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/codes/getAllCodeItems");
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
