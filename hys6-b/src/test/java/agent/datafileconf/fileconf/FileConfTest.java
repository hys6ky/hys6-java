package agent.datafileconf.fileconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class FileConfTest {

    @Test
    public void getInitDataFileData() {
        try {
            URL url = new URL("http://localhost:20003/B/fileconf/getInitDataFileData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976069813535244288&agent_id=913366574159630336";
            OutputStream outputStream = connection.getOutputStream();
            outputStream.write(params.getBytes());
            outputStream.flush();
            outputStream.close();
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "999");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void addDataFileData() {
        try {
            URL url = new URL("http://localhost:20003/B/fileconf/addDataFileData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976069813535244288&agent_id=913366574159630336";
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "193");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Test
    public void saveDataFile() {
        try {
            URL url = new URL("http://localhost:20003/B/fileconf/saveDataFile");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976069813535244288&agent_id=913366574159630336";
            int responseCode = connection.getResponseCode();
            System.out.println("Response code: " + responseCode);
            BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            String line;
            StringBuffer response = new StringBuffer();
            while ((line = reader.readLine()) != null) {
                response.append(line);
            }
            reader.close();
            connection.disconnect();
            System.out.println("Response content: " + response.toString());
            Map<String, Object> result = JsonUtil.toObject(response.toString(), new TypeReference<Map<String, Object>>() {
            });
            Assert.assertEquals(result.get("code").toString(), "193");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
