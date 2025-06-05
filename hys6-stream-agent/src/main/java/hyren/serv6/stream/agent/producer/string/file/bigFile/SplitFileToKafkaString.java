package hyren.serv6.stream.agent.producer.string.file.bigFile;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import hyren.serv6.stream.agent.producer.commons.KafkaProducerWorker;
import hyren.serv6.stream.agent.realtimecollection.filecontentstream.FileContentStreamInfoController;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.*;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.CountDownLatch;

public class SplitFileToKafkaString implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    public static final long READ_POSITION_LATEST = -2L;

    public static final long READ_POSITION_ASSIGN = 1L;

    private String jobId;

    private Map<String, Object> json;

    private String lineNums;

    private CountDownLatch countDownLatch;

    public SplitFileToKafkaString(String jobId, Map<String, Object> json, CountDownLatch countDownLatch) {
        this.jobId = jobId;
        this.json = json;
        this.countDownLatch = countDownLatch;
        this.lineNums = null;
    }

    public SplitFileToKafkaString(String jobId, Map<String, Object> json, String lineNums, CountDownLatch countDownLatch) {
        this.jobId = jobId;
        this.json = json;
        this.lineNums = lineNums;
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        Map<String, Object> jsonParams = (Map<String, Object>) json.get("sdm_receive_conf");
        String fileAbsolute = null;
        if (null != jsonParams.get("ra_file_path")) {
            fileAbsolute = jsonParams.get("ra_file_path").toString();
        }
        if (FileContentStreamInfoController.mapJob.containsKey(jobId)) {
            FileContentStreamInfoController.mapJob.get(jobId).interrupt();
        }
        FileContentStreamInfoController.mapJob.put(jobId, Thread.currentThread());
        ProducerOperatorBigFile producerOperatorBigFile = new ProducerOperatorBigFile();
        JobParamsEntity jobParams = producerOperatorBigFile.getMapParam(json, jobId);
        File srcFile = new File(fileAbsolute);
        if (!srcFile.exists()) {
            logger.info("ProducerOperatorBigFile------------------文件不存在！！！");
            Thread.currentThread().interrupt();
        } else {
            long position = 0;
            if (null != jsonParams.get("file_initposition")) {
                position = Long.parseLong(jsonParams.get("file_initposition").toString());
            }
            int filePointer = 0;
            RandomAccessFile rafFilePointer = null;
            InputStream in = null;
            BufferedInputStream bis = null;
            try {
                rafFilePointer = new RandomAccessFile(new File(fileAbsolute + ".rdp"), "rw");
                if (position == READ_POSITION_LATEST) {
                    rafFilePointer.seek(0);
                    filePointer = rafFilePointer.readInt();
                } else if (position == READ_POSITION_ASSIGN) {
                    String file_read_num = null;
                    if (null != jsonParams.get("file_read_num")) {
                        file_read_num = jsonParams.get("file_read_num").toString();
                    }
                    filePointer = Integer.parseInt(file_read_num);
                }
                long countSize = srcFile.length();
                long fileSize = 0;
                if (null != jsonParams.get("messageSize")) {
                    fileSize = Long.parseLong(jsonParams.get("messageSize").toString());
                }
                int num;
                if (countSize % fileSize == 0) {
                    num = (int) (countSize / fileSize);
                } else {
                    num = (int) (countSize / fileSize) + 1;
                }
                in = new FileInputStream(srcFile);
                bis = new BufferedInputStream(in);
                byte[] bytes = new byte[(int) fileSize];
                int len;
                int count = 0;
                KafkaProducerWorker kafkaProducerWorker = new KafkaProducerWorker();
                List<String> linuNumList = new ArrayList<>();
                if (!StringUtil.isBlank(lineNums)) {
                    linuNumList = Arrays.asList(lineNums.split(","));
                }
                int i = 0;
                while ((len = bis.read(bytes)) != -1) {
                    if ((i >= filePointer && linuNumList.isEmpty()) || linuNumList.contains(String.valueOf(i))) {
                        Map<String, Object> jsonObject = new HashMap<>();
                        List<Byte> sendBytes = new ArrayList<>();
                        for (int j = 0; j < bytes.length; j++) {
                            if (bytes[j] != 0) {
                                sendBytes.add(bytes[j]);
                            }
                        }
                        jsonObject.put("line", String.valueOf(len));
                        jsonObject.put("bytes", JsonUtil.toJson(sendBytes));
                        jsonObject.put("lineNum", i);
                        if (kafkaProducerWorker.sendToKafka(fileAbsolute, jobParams.getProducerString(), jsonObject.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync())) {
                            rafFilePointer.seek(0);
                            rafFilePointer.writeLong(i);
                        }
                        count += len;
                        if (count >= countSize) {
                            break;
                        }
                    }
                    i++;
                }
                StringBuilder sb = new StringBuilder();
                String fileType = Files.probeContentType(srcFile.toPath());
                sb.append(num).append(",").append(srcFile.length()).append(",").append(fileAbsolute).append(",").append(fileType).append(",").append(Constant.STREAM_HYREN_END).append(",").append(jobId);
                sb.append(",").append("md5");
                String sdm_server_ip = null;
                if (null != jsonParams.get("sdm_server_ip")) {
                    sdm_server_ip = jsonParams.get("sdm_server_ip").toString();
                }
                String sdm_rec_port = null;
                if (null != jsonParams.get("sdm_rec_port")) {
                    sdm_rec_port = jsonParams.get("sdm_rec_port").toString();
                }
                sb.append(",").append(sdm_server_ip).append(":").append(sdm_rec_port);
                Map<String, Object> jsonObject = new HashMap<>();
                jsonObject.put("line", sb.toString());
                jsonObject.put("lineNum", 0);
                kafkaProducerWorker.sendToKafka(fileAbsolute, jobParams.getProducerString(), jsonObject.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync());
                logger.info("ProducerOperatorBigFile----------------read file end:" + System.currentTimeMillis());
                if (this.countDownLatch != null) {
                    this.countDownLatch.countDown();
                }
            } catch (Exception e) {
                logger.error("ProducerOperatorBigFile-------------文件读取异常！！！", e);
                throw new BusinessException("ProducerOperatorBigFile----------------文件读取异常！！！" + e.getMessage());
            } finally {
                if (rafFilePointer != null && bis != null && in != null) {
                    try {
                        rafFilePointer.close();
                        bis.close();
                        in.close();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }
}
