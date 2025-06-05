package hyren.serv6.commons.utils.stream;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.kafka.common.errors.SerializationException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class ByteToString {

    private String encoding = "UTF8";

    public String byteToString(byte[] data) {
        try {
            if (data == null) {
                return null;
            }
            return new String(data, this.encoding);
        } catch (UnsupportedEncodingException e) {
            throw new SerializationException("Error when deserializing byte[] to string due to unsupported encoding " + this.encoding);
        }
    }

    public Map<String, Object> byteToMap(byte[] data) {
        String str = byteToString(data);
        Map<String, Object> jsonObject = new HashMap<>();
        if (str.length() <= 2) {
            return null;
        }
        try {
            if (str.contains(Constant.STREAM_HYREN_END)) {
                return null;
            } else if (str.contains("line") && str.contains("lineNum")) {
                String lineNum = str.substring(str.indexOf("bytes=") + 6, str.indexOf("lineNum") - 2);
                byte[] bytes = JsonUtil.toObject(lineNum, new TypeReference<byte[]>() {
                });
                String readText = new String(bytes);
                jsonObject.put("line", readText);
            } else if (str.contains("line")) {
                String substring = str.substring(1, str.length() - 1);
                String[] strings = substring.trim().split(";");
                jsonObject = Arrays.stream(strings).map(s -> s.split("=")).collect(Collectors.toMap(strings1 -> strings1[0].trim(), strings1 -> strings1[1]));
            } else {
                String substring = str.substring(1, str.length() - 1);
                String[] strings = substring.trim().split(",");
                jsonObject = Arrays.stream(strings).map(s -> s.split("=")).collect(Collectors.toMap(strings1 -> strings1[0].trim(), strings1 -> strings1[1]));
            }
        } catch (Exception e) {
            throw new BusinessException("json format failed...");
        }
        return jsonObject;
    }
}
