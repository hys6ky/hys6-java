package hyren.serv6.a.bigcreendisplay;

import hyren.serv6.a.AppMain;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.server.LocalServerPort;
import org.springframework.test.context.junit4.SpringRunner;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;

@RunWith(SpringRunner.class)
@SpringBootTest(classes = AppMain.class, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BigScreenDisplayTest {

    @LocalServerPort
    private int port;

    @Test
    public void failuresJobNum() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/failuresJobNum");
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

    @Test
    public void totalSystemCapacity() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/totalSystemCapacity?yearData=1987");
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

    @Test
    public void totalNumberOfData() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/totalNumberOfData?yearData=1987");
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

    @Test
    public void totalNumberOfConnectedSystems() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/totalNumberOfConnectedSystems");
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

    @Test
    public void totalNumberOfAccessDatabases() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/totalNumberOfAccessDatabases");
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

    @Test
    public void totalNumberOfAccessDataTables() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/totalNumberOfAccessDataTables");
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

    @Test
    public void totalNumberOfCollect() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/totalNumberOfCollect");
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

    @Test
    public void numberOfTableCollectedToday() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/numberOfTableCollectedToday");
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

    @Test
    public void numberOfFailedCollectionTablesToday() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/numberOfFailedCollectionTablesToday");
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

    @Test
    public void newAccessData() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/newAccessData");
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

    @Test
    public void dataProcessingVolume() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/dataProcessingVolume");
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

    @Test
    public void numberOfTablesAfterProcessing() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/numberOfTablesAfterProcessing");
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

    @Test
    public void dataSchedulingSituation() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/dataSchedulingSituation");
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

    @Test
    public void interfaceCallSituation() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/interfaceCallSituation");
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

    @Test
    public void getResourceInfo() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/getResourceInfo");
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

    @Test
    public void collectTotalSize() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/collectTotalSize");
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

    @Test
    public void datasourceAndCollectTotalSize() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/datasourceAndCollectTotalSize");
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

    @Test
    public void numberOfInterfaceToday() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/numberOfInterfaceToday");
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

    @Test
    public void newAccessDatabaseYear() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/newAccessDatabaseYear");
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

    @Test
    public void numberOfFileToday() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/numberOfFileToday");
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

    @Test
    public void numberOfFileYear() {
        try {
            URL url = new URL("http://localhost:" + port + "/A/bigscreendisplay/numberOfFileYear");
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
