package hyren.serv6.stream.agent.producer.commons;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.IsFlag;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetFileParams {

    private static final Logger logger = LogManager.getLogger();

    public SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddhhmmss");

    public GenericRecord getRealGenericRecordAvro(JobParamsEntity jobParams, StringBuilder lineBuffer, List<String> listColumn, GenericRecord genericRecord) throws Exception {
        final byte[] bytes = lineBuffer.toString().getBytes();
        String message = Arrays.toString(bytes);
        String sdmDatelimiter = jobParams.getSdmDatelimiter();
        if (!StringUtil.isBlank(sdmDatelimiter)) {
            List<String> column_s = StringUtil.split(message, sdmDatelimiter);
            for (int i = 0; i < column_s.size(); i++) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(listColumn.get(i).split("`")[1])) {
                    genericRecord.put(listColumn.get(i).split("`")[0], column_s.get(i));
                }
            }
        } else {
            genericRecord.put("line", message);
        }
        String cusDesType = jobParams.getCusDesType();
        if (!StringUtil.isBlank(cusDesType)) {
            if (IsFlag.Shi == IsFlag.ofEnumByCode(cusDesType)) {
                genericRecord = jobParams.getBusinessProcess().process(listColumn, genericRecord);
            } else {
                genericRecord = (GenericRecord) jobParams.getInvocable().invokeFunction("recordFunction", genericRecord);
            }
        }
        return genericRecord;
    }

    public GenericRecord getRealGenericRecordAvro(JobParamsEntity jobParams, StringBuilder lineBuffer, List<String> listColumn, GenericRecord genericRecord, String read_mode) throws Exception {
        String message = new String(lineBuffer.toString().getBytes(DataBaseCode.UTF_8.getValue()), DataBaseCode.UTF_8.getValue());
        if ("1".equals(read_mode)) {
            String sdmDatelimiter = jobParams.getSdmDatelimiter();
            if (!StringUtil.isBlank(sdmDatelimiter)) {
                List<String> column_s = StringUtil.split(message, sdmDatelimiter);
                for (int i = 0; i < column_s.size(); i++) {
                    if (IsFlag.Shi == IsFlag.ofEnumByCode(listColumn.get(i).split("`")[1])) {
                        genericRecord.put(listColumn.get(i).split("`")[0], column_s.get(i));
                    }
                }
            } else {
                genericRecord.put("line", message);
            }
        } else {
            boolean flag = true;
            Map<String, Object> object = JsonUtil.toObject(message, new TypeReference<Map<String, Object>>() {
            });
            for (String column : listColumn) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                    flag = false;
                    genericRecord.put(column.split("`")[0], object.get(column.split("`")[0]).toString());
                }
            }
            if (flag) {
                genericRecord.put("line", message);
            }
        }
        String cusDesType = jobParams.getCusDesType();
        if (StringUtil.isNotBlank(cusDesType)) {
            if ("1".equals(cusDesType)) {
                genericRecord = jobParams.getBusinessProcess().process(listColumn, genericRecord);
            } else {
                genericRecord = (GenericRecord) jobParams.getInvocable().invokeFunction("recordFunction", genericRecord);
            }
        }
        return genericRecord;
    }

    public GenericRecord getParmGenericRecordAvro(List<String> listColumn, File file, GenericRecord genericRecord) {
        if (listColumn.contains("file_attr_ip`1")) {
            String iplocal = getIp();
            genericRecord.put("file_attr_ip", iplocal);
        }
        if (listColumn.contains("file_name`1")) {
            genericRecord.put("file_name", file.getName());
        }
        if (listColumn.contains("file_size`1")) {
            genericRecord.put("file_size", String.valueOf(file.length()));
        }
        if (listColumn.contains("file_time`1")) {
            genericRecord.put("file_time", sdf.format(file.lastModified()));
        }
        if (listColumn.contains("full_path`1")) {
            genericRecord.put("full_path", file.getParent());
        }
        return genericRecord;
    }

    public Map<String, Object> getRealJson(JobParamsEntity jobParams, StringBuilder lineBuffer, String charset, List<String> listColumn, Map<String, Object> json, String read_mode) throws Exception {
        Map<String, Object> jsonReal = new HashMap<>();
        jsonReal.putAll(json);
        String message = new String(lineBuffer.toString().getBytes(StandardCharsets.ISO_8859_1), DataBaseCode.ofValueByCode(charset));
        if ("1".equals(read_mode)) {
            String sdmDatelimiter = jobParams.getSdmDatelimiter();
            if (!StringUtil.isBlank(sdmDatelimiter)) {
                List<String> columns = StringUtil.split(message, sdmDatelimiter);
                int i = 0;
                for (String column : columns) {
                    if (IsFlag.Shi == IsFlag.ofEnumByCode(listColumn.get(i).split("`")[1])) {
                        jsonReal.put(listColumn.get(i).split("`")[0], column);
                    }
                    i++;
                }
            } else {
                jsonReal.put("line", message);
            }
        } else {
            boolean flag = true;
            Map<String, Object> object = JsonUtil.toObject(message, new TypeReference<Map<String, Object>>() {
            });
            for (String column : listColumn) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                    flag = false;
                    jsonReal.put(column.split("`")[0], object.get(column.split("`")[0]).toString());
                }
            }
            if (flag) {
                jsonReal.put("line", message);
            }
        }
        String cusDesType = jobParams.getCusDesType();
        if ("1".equals(cusDesType)) {
            jsonReal = jobParams.getBusinessProcess().process(listColumn, jsonReal);
        } else if ("2".equals(cusDesType)) {
            Object recordFunction = jobParams.getInvocable().invokeFunction("recordFunction", JsonUtil.toJson(jsonReal).trim());
            String replaceStr = recordFunction.toString().trim().replace("\\", "");
            String obj = replaceStr.substring(1, replaceStr.length() - 1);
            jsonReal = JsonUtil.toObject(obj, new TypeReference<Map<String, Object>>() {
            });
        }
        return jsonReal;
    }

    public Map<String, Object> getRealJson(JobParamsEntity jobParams, StringBuilder lineBuffer, List<String> listColumn, Map<String, Object> json) throws Exception {
        Map<String, Object> jsonReal = new HashMap<>();
        jsonReal.putAll(json);
        byte[] bytes = lineBuffer.toString().getBytes();
        String message = Arrays.toString(bytes);
        String sdmDatelimiter = jobParams.getSdmDatelimiter();
        if (!StringUtil.isBlank(sdmDatelimiter)) {
            List<String> column_s = StringUtil.split(message, sdmDatelimiter);
            for (int i = 0; i < column_s.size(); i++) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(listColumn.get(i).split("`")[1])) {
                    jsonReal.put(listColumn.get(i).split("`")[0], column_s.get(i));
                }
            }
        } else {
            jsonReal.put("line", message);
        }
        String cusDesType = jobParams.getCusDesType();
        if (!StringUtil.isBlank(cusDesType)) {
            if (IsFlag.Shi == IsFlag.ofEnumByCode(cusDesType)) {
                jsonReal = jobParams.getBusinessProcess().process(listColumn, jsonReal);
            } else {
                jsonReal = (Map<String, Object>) jobParams.getInvocable().invokeFunction("recordFunction", jsonReal);
            }
        }
        return jsonReal;
    }

    public Map<String, Object> getParmJson(List<String> listColumn, File file) {
        Map<String, Object> json = new HashMap<>();
        if (listColumn.contains("file_attr_ip`1")) {
            String ipLocal = getIp();
            json.put("file_attr_ip", ipLocal);
        }
        if (listColumn.contains("file_name`1")) {
            json.put("file_name", file.getName());
        }
        if (listColumn.contains("file_size`1")) {
            json.put("file_size", String.valueOf(file.length()));
        }
        if (listColumn.contains("file_time`1")) {
            json.put("file_time", String.valueOf(file.lastModified()));
        }
        if (listColumn.contains("full_path`1")) {
            json.put("full_path", file.getParent());
        }
        return json;
    }

    private String getIp() {
        InetAddress addr;
        String iplocal = null;
        try {
            addr = InetAddress.getLocalHost();
            iplocal = addr.getHostAddress();
        } catch (UnknownHostException e1) {
            logger.error("GetFileParams-----------------------获取主机ip异常：", e1);
            System.exit(-2);
        }
        return iplocal;
    }
}
