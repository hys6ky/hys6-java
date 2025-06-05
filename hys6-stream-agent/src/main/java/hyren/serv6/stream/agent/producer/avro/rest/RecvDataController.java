package hyren.serv6.stream.agent.producer.avro.rest;

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
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.tomcat.util.http.fileupload.FileItem;
import org.apache.tomcat.util.http.fileupload.disk.DiskFileItemFactory;
import org.apache.tomcat.util.http.fileupload.servlet.ServletFileUpload;
import org.apache.tomcat.util.http.fileupload.servlet.ServletRequestContext;
import javax.servlet.http.HttpServletRequest;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;

public class RecvDataController {

    private static final Logger logger = LogManager.getLogger();

    public void acceptorService(HttpServletRequest request, JobParamsEntity jobParams) {
        Response_result resp_json = new Response_result();
        String backData = null;
        String message = null;
        List<String> listColumn = null;
        GenericRecord genericRecord = new GenericData.Record(jobParams.getSchema());
        try {
            boolean isMultipart = ServletFileUpload.isMultipartContent(request);
            if (isMultipart) {
                getFileStream(genericRecord, request);
            } else {
                listColumn = jobParams.getListColumn();
                if (IsFlag.Shi == IsFlag.ofEnumByCode(jobParams.getMsgType())) {
                    String value = request.getParameter(jobParams.getMsgHeader());
                    Map<String, Object> jsonValue = JsonUtil.toObject(value, new TypeReference<Map<String, Object>>() {
                    });
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            String parameter = jsonValue.get(column.split("`")[0]).toString();
                            genericRecord.put(column.split("`")[0], parameter);
                        }
                    }
                } else {
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            Object parameter = request.getParameter(column.split("`")[0]);
                            genericRecord.put(column.split("`")[0], parameter);
                        }
                    }
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
            KafkaProducerWorker kafkaProducerWorker = new KafkaProducerWorker();
            if (!kafkaProducerWorker.sendToKafka(request.getRequestURL().toString(), jobParams.getProducer(), genericRecord, jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync())) {
                logger.error("RecvDataController-------------------进kafka失败！！！");
                message = "kafka缓存时出现错误！！！";
                resp_json.setMessage(message);
            }
            StatusDefine.success(resp_json, backData, message);
        } catch (BusinessException e) {
            logger.error("RecvDataController---------------结构化数据不能转换为json字符串，请使用json字符串上传结构化数据", e);
            message = "结构化数据不能转换为json字符串，请使用json字符串上传结构化数据。";
            StatusDefine.error(resp_json, e, message);
        } catch (ClassCastException e) {
            logger.error("RecvDataController----------------------不是多表单请求，请使用多表单请求上传文件", e);
            message = "不是多表单请求，请使用多表单请求上传文件。";
            StatusDefine.error(resp_json, e, message);
        } catch (NullPointerException e) {
            logger.error("RecvDataController-----------------未知的关键字为空，请优先确认jobKey是否存在", e);
            message = "未知的关键字为空，请优先确认jobKey是否存在";
            StatusDefine.error(resp_json, e, message);
        } catch (Exception e) {
            logger.error("RecvDataController-------------------未知的服务端错误", e);
            message = "未知的服务端错误";
            StatusDefine.fail(resp_json, e, message);
        }
    }

    private static void getFileStream(GenericRecord genericRecord, HttpServletRequest request) {
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
                    genericRecord.put(fieldName, ByteBuffer.wrap(byteArray));
                } else {
                    String msg = fileItem.getString();
                    genericRecord.put(fieldName, msg);
                }
            }
        } catch (BusinessException | IOException e) {
            logger.error(e);
            throw new BusinessException(e.getMessage());
        }
    }
}
