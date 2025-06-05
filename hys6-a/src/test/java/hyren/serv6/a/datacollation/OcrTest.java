package hyren.serv6.a.datacollation;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

@SpringBootTest
@RunWith(SpringRunner.class)
public class OcrTest {

    @Test
    public void getFileCollectionDataSources() {
        try {
            URL url = new URL("http://localhost:18000/A/datacollation/dataAssociation/executeSolrDataAssociation?relationTableName=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
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
