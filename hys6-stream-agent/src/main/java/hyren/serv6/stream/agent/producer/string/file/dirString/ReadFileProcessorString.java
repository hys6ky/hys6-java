package hyren.serv6.stream.agent.producer.string.file.dirString;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.stream.agent.producer.commons.FileDataValidator;
import hyren.serv6.stream.agent.producer.commons.GetFileParams;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import hyren.serv6.stream.agent.producer.commons.KafkaProducerWorker;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class ReadFileProcessorString {

    private static final Logger logger = LogManager.getLogger();

    public void lineProcessor(GetFileParams getFileParams, MapDBHelper mapDBHelper, ConcurrentMap<String, String> htmap, ConcurrentMap<String, String> htMapThread, File file, List<String> listColumn, JobParamsEntity jobParams, File fileRename, String beforeLine, Map<String, Object> json, FileDataValidator fileDataValidator) {
        RandomAccessFile readFile;
        try {
            readFile = new RandomAccessFile(file, "r");
            StringBuilder lineBuffer = new StringBuilder();
            if (beforeLine != null) {
                readFile.seek(Long.parseLong(beforeLine));
            }
            KafkaProducerWorker kafkaProducerWorkerString = new KafkaProducerWorker();
            long number = 0;
            while (true) {
                number++;
                String line = readFile.readLine();
                if (line != null) {
                    if (isNewLine(line, fileDataValidator)) {
                        if (lineBuffer.length() < 1) {
                            lineBuffer.append(line);
                        } else {
                            json = getFileParams.getRealJson(jobParams, lineBuffer, listColumn, json);
                            if (kafkaProducerWorkerString.sendToKafka(file.getAbsolutePath(), jobParams.getProducerString(), json.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync())) {
                                htmap.put(file.getName(), String.valueOf(readFile.getFilePointer()));
                                lineBuffer.delete(0, lineBuffer.length());
                                lineBuffer.append(line);
                                if (number % 5000 == 0) {
                                    mapDBHelper.commit();
                                }
                            } else {
                                logger.error("数据发送失败！！！");
                                break;
                            }
                        }
                    } else {
                        lineBuffer.append("\n").append(line);
                    }
                } else {
                    if (lineBuffer.length() > 0) {
                        json = getFileParams.getRealJson(jobParams, lineBuffer, listColumn, json);
                        kafkaProducerWorkerString.sendToKafka(file.getAbsolutePath(), jobParams.getProducerString(), json.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync());
                    }
                    IOUtils.closeQuietly(readFile);
                    htmap.put(file.getName(), "all");
                    mapDBHelper.commit();
                    try {
                        File fileOld = new File((fileRename.getAbsolutePath() + File.separator + file.getName()));
                        if (FileUtils.directoryContains(fileRename, fileOld)) {
                            if (fileOld.delete()) {
                                throw new BusinessException("删除旧文件失败：" + fileOld.getPath());
                            }
                        }
                        FileUtils.moveToDirectory(file, fileRename, true);
                        htMapThread.remove(file.getName());
                    } catch (Exception e) {
                        logger.error("文件移动失败！！！失败文件为：" + file.getAbsolutePath(), e);
                    }
                    break;
                }
            }
        } catch (Exception e) {
            logger.error(e);
            throw new BusinessException(e.getMessage());
        } finally {
            mapDBHelper.commit();
        }
    }

    public void objectProcessor(MapDBHelper mapDBHelper, ConcurrentMap<String, String> htMap, ConcurrentMap<String, String> htMapThread, File file, List<String> listColumn, JobParamsEntity jobParams, File fileRename, String charset, Map<String, Object> json) {
        try {
            String message = readParam(file, charset);
            KafkaProducerWorker kafkaProducerWorkerString = new KafkaProducerWorker();
            List<Object> jsonArray = JsonUtil.toObject(message, new TypeReference<List<Object>>() {
            });
            if (jsonArray != null) {
                for (Object obj : jsonArray) {
                    if (IsFlag.Fou == IsFlag.ofEnumByCode(jobParams.getIsObj())) {
                        json.put("line", obj.toString());
                    } else {
                        Map<String, Object> jsonMessage = (Map<String, Object>) obj;
                        for (String column : listColumn) {
                            if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                                String columnValue = jsonMessage.get(column.split("`")[0]).toString();
                                if (columnValue != null) {
                                    json.put(column.split("`")[0], columnValue);
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
                    kafkaProducerWorkerString.sendToKafka(file.getAbsolutePath(), jobParams.getProducerString(), json.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync());
                }
            } else {
                logger.error("数据格式错误，为非JsonArray数据！！！ 文件为：" + file.getAbsolutePath());
            }
            htMap.put(file.getName(), "all");
            mapDBHelper.commit();
        } catch (Exception e) {
            logger.error("file:" + file.getAbsolutePath(), e);
        }
        try {
            File fileOld = new File((fileRename.getAbsolutePath() + File.separator + file.getName()));
            if (FileUtils.directoryContains(fileRename, fileOld)) {
                if (!fileOld.delete()) {
                    throw new BusinessException("删除旧文件失败！！" + fileOld.getPath());
                }
            }
            FileUtils.moveToDirectory(file, fileRename, true);
            htMapThread.remove(file.getName());
        } catch (IOException e) {
            logger.error("文件移动失败！！！ 失败文件为：" + file.getAbsolutePath(), e);
        }
    }

    public String readParam(File file, String charset) {
        String lineTxt;
        StringBuilder stringBuilder = new StringBuilder();
        BufferedReader bf = null;
        try {
            bf = new BufferedReader(new FileReader(file));
            while ((lineTxt = bf.readLine()) != null) {
                stringBuilder.append(lineTxt);
            }
        } catch (FileNotFoundException e) {
            logger.error("文件不存在！！！失败文件为：" + file.getAbsolutePath(), e);
            System.exit(-2);
        } catch (IOException e1) {
            throw new BusinessException(e1.getMessage());
        } finally {
            IOUtils.closeQuietly(bf);
        }
        String message = null;
        try {
            message = new String(stringBuilder.toString().getBytes(DataBaseCode.ISO_8859_1.getValue()), DataBaseCode.ofValueByCode(charset));
        } catch (UnsupportedEncodingException e) {
            logger.error("数据类型转换失败！！！失败文件为：" + file.getAbsolutePath(), e);
        }
        return message;
    }

    private boolean isNewLine(String line, FileDataValidator fileDataValidator) {
        if (fileDataValidator != null) {
            return fileDataValidator.isNewLine(line);
        } else {
            return true;
        }
    }
}
