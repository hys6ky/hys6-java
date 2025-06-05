package hyren.serv6.h;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.time.LocalDateTime;
import java.util.Date;
import java.util.Map;
import java.util.StringJoiner;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

public class HttpUtils {

    public static String sendPost(String url, Map<String, String> params, Object body, String token) throws Exception {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = URLEncoder.encode(entry.getKey(), "UTF-8");
            String value = entry.getValue() == null ? "" : URLEncoder.encode(entry.getValue(), "UTF-8");
            sj.add(key + "=" + value);
        }
        url += sj.length() == 0 ? "" : ("?" + sj.toString());
        System.out.println("\t" + url);
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("HyrenAuthorization", token);
        con.setRequestProperty("Accept", "*/*");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);
        if (body != null) {
            try (OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8")) {
                writer.write(toJson(body));
                writer.flush();
            }
        }
        try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        }
    }

    public static String toJson(Object obj) throws JsonProcessingException {
        return new ObjectMapper().writeValueAsString(obj);
    }

    public static String download(String url, Map<String, String> params, Object body, String token, String downloadFilePath) throws Exception {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = URLEncoder.encode(entry.getKey(), "UTF-8");
            String value = entry.getValue() == null ? "" : URLEncoder.encode(entry.getValue(), "UTF-8");
            sj.add(key + "=" + value);
        }
        url += sj.length() == 0 ? "" : ("?" + sj.toString());
        System.out.println("\t" + url);
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("HyrenAuthorization", token);
        con.setRequestProperty("Accept", "*/*");
        con.setRequestProperty("Content-Type", "application/json");
        con.setDoOutput(true);
        if (body != null) {
            try (OutputStreamWriter writer = new OutputStreamWriter(con.getOutputStream(), "UTF-8")) {
                writer.write(toJson(body));
                writer.flush();
            }
        }
        try (InputStream in = con.getInputStream()) {
            File folder = new File(downloadFilePath);
            if (!folder.isDirectory()) {
                throw new Exception("文件夹不存在");
            }
            String fileName = "temp_" + new Date().getTime() + ".txt";
            File file = new File(folder, fileName);
            if (!file.createNewFile()) {
                throw new Exception("创建临时文件失败");
            }
            FileOutputStream output = new FileOutputStream(file);
            byte[] b = new byte[1024];
            int flag = 0;
            while ((flag = in.read(b)) != -1) {
                System.out.println(new String(b));
                output.write(b);
            }
            in.close();
            output.close();
            return file.getAbsolutePath();
        }
    }
}
