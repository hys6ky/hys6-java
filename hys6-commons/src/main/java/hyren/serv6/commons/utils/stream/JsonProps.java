package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import org.apache.commons.io.IOUtils;
import java.io.*;
import java.util.Map;

public class JsonProps {

    public Map<String, Object> jsonProperties(String path) {
        Map<String, Object> json = null;
        try {
            json = JsonUtil.toObject(IOUtils.toString(new FileInputStream(new File(path))), new TypeReference<Map<String, Object>>() {
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return json;
    }

    public Map<String, Object> getParam(String path) {
        String lineTxt = null;
        String json = null;
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(path));
            while ((lineTxt = bf.readLine()) != null) {
                json = lineTxt;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (bf != null) {
                IOUtils.closeQuietly(bf);
            }
        }
        return JsonUtil.toObject(json, new TypeReference<Map<String, Object>>() {
        });
    }
}
