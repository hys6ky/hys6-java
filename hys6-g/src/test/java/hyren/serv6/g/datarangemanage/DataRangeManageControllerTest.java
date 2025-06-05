package hyren.serv6.g.datarangemanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import org.junit.BeforeClass;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class DataRangeManageControllerTest {

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
    public void testSearchDataUsageRangeInfoToTreeData() {
        try {
            URL url = new URL("http://localhost:20001/G/datarangemanage/searchDataUsageRangeInfoToTreeData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty(tokenName, token);
            connection.setDoOutput(true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            System.out.println(response);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testSearchFieldById() {
        try {
            URL url = new URL("http://localhost:20001/G/datarangemanage/searchFieldById");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty(tokenName, token);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.connect();
            String body = "data_layer=DCL&file_id=e0cb1416-f3c5-455e-8ac6-d957fe1507c0";
            BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
            bw.write(body);
            bw.close();
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
    public void testSaveTableData() {
        try {
            URL url = new URL("http://localhost:20001/G/datarangemanage/saveTableData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty(tokenName, token);
            connection.setDoOutput(true);
            connection.setDoInput(true);
            connection.setUseCaches(false);
            connection.connect();
            String body = "tableDataInfos=[%7B%22file_id%22:%22e0cb1416-f3c5-455e-8ac6-d957fe1507c0%22," + "%22table_ch_column%22:[%22HYREN_E_DATE%22,%22HYREN_S_DATE%22,%22HYREN_OPER_PERSON%22," + "%22REMARK%22,%22PARA_NAME%22,%22HYREN_OPER_DATE%22,%22PARA_VALUE%22,%22HYREN_MD5_VAL%22," + "%22PARA_ID%22,%22HYREN_OPER_TIME%22,%22PARA_TYPE%22],%22table_en_column%22:" + "[%22HYREN_E_DATE%22,%22HYREN_S_DATE%22,%22HYREN_OPER_PERSON%22,%22REMARK%22," + "%22PARA_NAME%22,%22HYREN_OPER_DATE%22,%22PARA_VALUE%22,%22HYREN_MD5_VAL%22,%22PARA_ID%22," + "%22HYREN_OPER_TIME%22,%22PARA_TYPE%22]%7D]&data_layer=DCL&user_id[]=5298&table_note=";
            BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(connection.getOutputStream(), "UTF-8"));
            writer.write(body);
            writer.close();
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String line;
                StringBuffer stringBuffer = new StringBuffer();
                while ((line = reader.readLine()) != null) {
                    stringBuffer.append(line);
                }
                reader.close();
                System.out.println(stringBuffer);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
