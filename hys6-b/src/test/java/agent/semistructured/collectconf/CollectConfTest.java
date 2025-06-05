package agent.semistructured.collectconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.utils.JsonUtil;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class CollectConfTest {

    String encode = "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI";

    @Test
    @Method(desc = "", logicStep = "")
    public void searchFileSource() {
        try {
            String re_url = "http://127.0.0.1:20003/B/collectconf/getInitObjectCollectConf?agent_id=" + 986927274492170240L;
            URL url = new URL(re_url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", encode);
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Method(desc = "", logicStep = "")
    public void getAddObjectCollectConf() {
        try {
            String re_url = "http://127.0.0.1:20003/B/collectconf/getAddObjectCollectConf?agent_id=" + 986927274492170240L;
            URL url = new URL(re_url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", encode);
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Method(desc = "", logicStep = "")
    public void getObjectCollectConfById() {
        try {
            String re_url = "http://127.0.0.1:20003/B/collectconf/getObjectCollectConfById?odc_id=" + 1062419609564483584L;
            URL url = new URL(re_url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", encode);
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    @Method(desc = "", logicStep = "")
    public void saveObjectCollect() {
        try {
            String re_url = "http://127.0.0.1:20003/B/collectconf/saveObjectCollect?odc_id=" + 1L + "&object_collect_type=1&" + "obj_number=100&obj_collect_name=test1367&system_name=Linux&host_name=root&local_time=20230110171525&server_date=20230110171525" + "&s_date=20230110&e_date=20230518&database_code=1&file_path=/home/hyshf/dhw/bjgh&is_dictionary=1&is_sendok=0&data_date=20230110" + "&file_suffix=dat&agent_id=" + 1L;
            URL url = new URL(re_url);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", encode);
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            System.out.println("========================================");
            System.out.println(result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
