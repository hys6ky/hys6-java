package agent.dbagentconf.startwayconf;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import org.junit.Assert;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class StartwayconfTest {

    @Test
    public void getDatabaseInfo() {
        try {
            URL url = new URL("http://localhost:20003/B/startwayconf/getEtlSysData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976069813535244288";
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
    public void getEtlSubSysData() {
        try {
            URL url = new URL("http://localhost:20003/B/startwayconf/getEtlSubSysData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "etl_sys_cd=A123";
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
    public void getPreviewJob() {
        try {
            URL url = new URL("http://localhost:20003/B/startwayconf/getPreviewJob");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976483279622377472";
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
    public void getEtlJobData() {
        try {
            URL url = new URL("http://localhost:20003/B/startwayconf/getEtlJobData");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976483279622377472";
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
    public void getAgentPath() {
        try {
            URL url = new URL("http://localhost:20003/B/startwayconf/getAgentPath");
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("POST");
            connection.setDoOutput(true);
            connection.setRequestProperty("HyrenAuthedForBizpotOnlineUserinfo", "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI=");
            connection.setRequestProperty("User-Agent", "Mozilla/5.0");
            String params = "colSetId=976483279622377472";
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
}
