package hyren.serv6.g.releasemanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class ReleaseManageControllerTest {

    private static String token;

    private static String tokenName = "HyrenAuthorization";

    @BeforeClass
    public static void login() {
        try {
            URL url = new URL("http://localhost:20001/hyren-gateway/login?user_id=1001&password=1");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> jsonObject = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Object object = jsonObject.get("data");
            if (object instanceof Map) {
                Map<String, String> map = (Map<String, String>) object;
                token = map.get("accessToken");
            }
            System.out.println("token = " + token);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSearchUserInfo() {
        try {
            URL url = new URL("http://localhost:20001/G/releasemanage/searchUserInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty(tokenName, token);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.connect();
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                StringBuilder response = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                System.out.println(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSearchInterfaceInfoByType() {
        try {
            URL url = new URL("http://localhost:20001/G/releasemanage/searchInterfaceInfoByType?interface_type=2");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty(tokenName, token);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.connect();
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                StringBuilder response = new StringBuilder();
                while ((line = reader.readLine()) != null) {
                    response.append(line);
                }
                reader.close();
                System.out.println(response);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSaveInterfaceUseInfo() {
        try {
            URL url = new URL("http://localhost:20001/G/releasemanage/saveInterfaceUseInfo");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("HyrenAuthorization", "82829ec2a8606a20887550827459b87eb0777fc3de3a466e");
            String params = "userIds[]=5299&start_date=20230606&end_date=20230630&interfaceUses=[%7B%22interface_code%22:%2201-101%22,%22interface_id%22:101,%22interface_name%22:%22%E5%8D%95%E8%A1%A8%E6%99%AE%E9%80%9A%E6%9F%A5%E8%AF%A2%E6%8E%A5%E5%8F%A3%22,%22interface_state%22:%221%22,%22interface_type%22:%221%22,%22url%22:%22generalQuery%22,%22user_id%22:1001,%22start_use_date%22:%2220230606%22,%22use_valid_date%22:%2220230630%22%7D]";
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
}
