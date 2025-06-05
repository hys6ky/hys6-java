package hyren.serv6.stream.agent.producer.string.rest;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.CodecUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import hyren.serv6.stream.agent.producer.commons.KafkaProducerWorker;
import hyren.serv6.stream.agent.producer.commons.Response_result;
import hyren.serv6.stream.agent.producer.commons.StatusDefine;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.http.fileupload.FileItem;
import org.apache.tomcat.util.http.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.http.fileupload.servlet.ServletFileUpload;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;
import org.springframework.stereotype.Component;
import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

@Component
@Slf4j
public class RecvDataControllerString {

    private KafkaProducerWorker kafkaProducerWorkerString = new KafkaProducerWorker();

    public void acceptorService(HttpServletRequest request, JobParamsEntity jobParams) {
        Response_result resp_json = new Response_result();
        String message = null;
        List<String> listColumn = null;
        Map<String, Object> json = new HashMap<>();
        try {
            boolean isMultipart = ServletFileUpload.isMultipartContent(request);
            if (isMultipart) {
                getFileStream(json, request);
            } else {
                listColumn = jobParams.getListColumn();
                if (IsFlag.Shi == IsFlag.ofEnumByCode(jobParams.getMsgType())) {
                    BufferedReader reader = request.getReader();
                    String str;
                    StringBuilder sb = new StringBuilder();
                    while ((str = reader.readLine()) != null) {
                        sb.append(str);
                    }
                    reader.close();
                    Map<String, Object> jsonValue = JsonUtil.toObject(sb.toString(), new TypeReference<Map<String, Object>>() {
                    });
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            String colName = column.split("`")[0];
                            if (Objects.isNull(jsonValue.get(colName))) {
                                log.error("发送进kafka数据结构和主题任务结构不符或内容为空，不予生产。");
                                return;
                            }
                            json.put(column.split("`")[0], jsonValue.get(colName));
                        }
                    }
                } else {
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            Object parameter = request.getParameter(column.split("`")[0]);
                            if (Objects.isNull(parameter)) {
                                log.error("发送进kafka数据结构和主题任务结构不符，不予生产。");
                                Map<String, String> collect = listColumn.stream().map(s -> s.split("`")).collect(Collectors.toMap(strings -> strings[0], strings -> strings[1]));
                                log.error("结构为：{}", JsonUtil.toJson(collect));
                                return;
                            }
                            json.put(column.split("`")[0], parameter);
                        }
                    }
                }
            }
            String cusDesType = jobParams.getCusDesType();
            if (StringUtil.isNotBlank(cusDesType)) {
                if ("1".equals(cusDesType)) {
                    json = jobParams.getBusinessProcess().process(listColumn, json);
                } else {
                    Object recordFunction = jobParams.getInvocable().invokeFunction("recordFunction", JsonUtil.toJson(json).trim());
                    String replaceStr = recordFunction.toString().trim().replace("\\", "");
                    String objMsg = replaceStr.substring(1, replaceStr.length() - 1);
                    json = JsonUtil.toObject(objMsg, new TypeReference<Map<String, Object>>() {
                    });
                }
            }
            if (json.size() == 0) {
                log.error("发送进kafka数据结构和主题任务结构不符，不予生产。");
                log.error("RecvDataControllerString------------------进kafka失败！！！");
                return;
            } else if (!kafkaProducerWorkerString.sendToKafka(request.getRequestURL().toString(), jobParams.getProducerString(), json.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync())) {
                log.error("RecvDataControllerString------------------进kafka失败！！！");
                message = "kafka缓存时出现错误！！！";
                resp_json.setMessage(message);
            }
            StatusDefine.success(resp_json, null, message);
            log.info("RecvDataControllerString------------------进kafka成功！！！");
            log.info("RecvDataControllerString------------------data:{}", json.toString());
        } catch (BusinessException e) {
            log.error("RecvDataControllerString------------------结构化数据不能转换为json字符串，请使用json字符串上传结构化数据", e);
            message = "结构化数据不能转换为json字符串，请使用json字符串上传结构化数据。";
            StatusDefine.error(resp_json, e, message);
        } catch (ClassCastException e) {
            log.error("RecvDataControllerString--------------------------------------不是多表单请求，请使用多表单请求上传文件", e);
            message = "不是多表单请求，请使用多表单请求上传文件。";
            StatusDefine.error(resp_json, e, message);
        } catch (NullPointerException e) {
            log.error("RecvDataControllerString-----------------------------------未知的关键字为空，请优先确认jobKey是否存在", e);
            message = "未知的关键字为空，请优先确认jobKey是否存在";
            StatusDefine.error(resp_json, e, message);
        } catch (Exception e) {
            log.error("RecvDataControllerString-------------------------------------未知的服务端错误", e);
            message = "未知的服务端错误";
            StatusDefine.fail(resp_json, e, message);
        }
    }

    private static void getFileStream(Map<String, Object> json, HttpServletRequest request) {
        DiskFileItemFactory factory = new DiskFileItemFactory();
        ServletFileUpload upload = new ServletFileUpload(factory);
        upload.setFileSizeMax(50 * 1024 * 1024);
        upload.setSizeMax(50 * 1024 * 1024);
        upload.setHeaderEncoding(CodecUtil.UTF8_STRING);
        try {
            List<FileItem> list = upload.parseRequest(new ServletRequestContext(request));
            for (FileItem fileItem : list) {
                String fieldName = fileItem.getFieldName();
                if (!fileItem.isFormField()) {
                    InputStream inputStream = fileItem.getInputStream();
                    ByteArrayOutputStream bos = new ByteArrayOutputStream(1000);
                    byte[] b = new byte[1000];
                    int n;
                    while ((n = inputStream.read(b)) != -1) {
                        bos.write(b, 0, n);
                    }
                    bos.close();
                    byte[] byteArray = bos.toByteArray();
                    json.put(fieldName, ByteBuffer.wrap(byteArray));
                } else {
                    String msg = fileItem.getString();
                    json.put(fieldName, msg);
                }
            }
        } catch (BusinessException | IOException e) {
            log.error(e.getMessage(), e);
        }
    }
}
