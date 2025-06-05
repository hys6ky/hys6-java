package hyren.serv6.c;

import java.io.*;
import java.net.*;
import java.util.*;

public class HttpUtils {

    public static String sendPost(String url, Map<String, String> params, String token) throws Exception {
        StringJoiner sj = new StringJoiner("&");
        for (Map.Entry<String, String> entry : params.entrySet()) {
            String key = URLEncoder.encode(entry.getKey(), "UTF-8");
            String value = entry.getValue() == null ? "" : URLEncoder.encode(entry.getValue(), "UTF-8");
            sj.add(key + "=" + value);
        }
        url += "?" + sj.toString();
        System.out.println("\t" + url);
        URL obj = new URL(url);
        HttpURLConnection con = (HttpURLConnection) obj.openConnection();
        con.setRequestMethod("POST");
        con.setRequestProperty("HyrenAuthorization", token);
        con.setDoOutput(true);
        try (BufferedReader in = new BufferedReader(new InputStreamReader(con.getInputStream()))) {
            String line;
            StringBuilder response = new StringBuilder();
            while ((line = in.readLine()) != null) {
                response.append(line);
            }
            return response.toString();
        }
    }
}
