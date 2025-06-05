package dataquery;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.utils.JsonUtil;
import org.junit.Before;
import org.junit.Test;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Map;

public class DataQueryTest {

    private static long THREAD_ID = Thread.currentThread().getId();

    private static final String FILE_ID = "99999999999" + THREAD_ID;

    String encode = "AgAAATpLEkVDVUJeUV1VEgoSAgAAARIcElFGUVlcUVJcVRIKREJFVRwSRUNVQnlUEgoCAAABHBJCX1xVeVQSCgIAAAEcEkVDVUJ1XVFZXBIKElNTU3BGRh5TX10SHBJFQ1VCfV9SWVxVEgoSAQIDBAUGBwgJABIcEkVDVUJkSUBVEgpeRVxcHBJcX1dZXnlAEgpeRVxcHBJcX1dZXnRRRFUSCl5FXFwcEkVDVUJjRFFEVRIKEgESHBJUVUB5VBIKAQAAAAAAAAAAARwSVFVAflFdVRIKXkVcXBwSQl9cVX5RXVUSCl5FXFwcEkVDVUJkSUBVd0JfRUASCl5FXFwcEllDb1xfV1leEgoSABIcElxZXVlEfUVcRFl8X1dZXhIKEgESTZI";

    @Test
    @Method(desc = "", logicStep = "")
    public void searchDepartmentInfo() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getFileDataSource";
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
    public void getFileCollectionTask() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getFileCollectionTask?sourceId=" + 1112796254301065216L;
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
    public void downloadFile() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/downloadFile?fileId=" + FILE_ID + "&fileName=aaa&queryKeyword=cc";
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
    public void saveFavoriteFile() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/saveFavoriteFile?fileId=6ba32c10-4b21-498f-8eea-ef925d6fcb20";
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
    public void cancelFavoriteFile() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/cancelFavoriteFile?favId=" + 954335827633115136L;
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
    public void getFileClassifySum() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getFileClassifySum";
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
    public void getSevenDayCollectFileSum() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getSevenDayCollectFileSum";
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
    public void getLast3FileCollections() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getLast3FileCollections?timesRecently=" + 2;
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
    public void getConditionalQuery() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getConditionalQuery";
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
    public void applicationProcessing() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/applicationProcessing?fileId=6f30e19f-175d-4669-9e61-723876fad016&applyType=1";
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
    public void checkFileViewPermissions() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/checkFileViewPermissions?fileId=6f30e19f-175d-4669-9e61-723876fad016&applyType=1";
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
    public void viewFile() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/viewFile?fileId=6f30e19f-175d-4669-9e61-723876fad016&applyType=1";
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
    public void viewImage() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/viewImage?fileId=6f30e19f-175d-4669-9e61-723876fad016";
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
    public void getApplyData() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/getApplyData?apply_type=1";
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
    public void cancelApply() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/cancelApply?da_id=" + 1012394678802190336L;
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
    public void myApplyRecord() {
        try {
            String re_url = "http://127.0.0.1:20003/B/dataquery/myApplyRecord?currPage=1&pageSize=10";
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
