package hyren.serv6.stream.agent.producer.avro.file.dir;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.MapDBHelper;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.CusClassLoader;
import hyren.serv6.stream.agent.producer.commons.FileDataValidator;
import hyren.serv6.stream.agent.producer.commons.JobParamsEntity;
import hyren.serv6.stream.agent.producer.commons.KafkaProducerWorker;
import hyren.serv6.stream.agent.realtimecollection.filecontentstream.FileContentStreamInfoController;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DirListenerReader implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    public static final String folder = System.getProperty("user.dir");

    private ExecutorService executor;

    public boolean reading = true;

    private String jobId;

    private MapDBHelper mapDBHelper;

    private ConcurrentMap<String, String> htMap;

    private ConcurrentMap<String, String> htMapThread;

    private Map<String, Object> json;

    private CountDownLatch countDownLatch;

    public DirListenerReader(String jobId, Map<String, Object> json, CountDownLatch countDownLatch) {
        executor = FileContentStreamInfoController.mapExec.get(jobId);
        if (executor != null && !executor.isShutdown()) {
            FileContentStreamInfoController.mapStop.put(jobId, false);
            executor.shutdown();
            try {
                executor.awaitTermination(1, TimeUnit.HOURS);
            } catch (InterruptedException e) {
                logger.error(e);
            }
            mapDBHelper = FileContentStreamInfoController.mapMapDb.get(jobId);
            if (mapDBHelper != null) {
                mapDBHelper.close();
            }
        }
        this.json = json;
        this.jobId = jobId;
        FileContentStreamInfoController.mapStop.put(jobId, true);
        mapDBHelper = new MapDBHelper(folder + File.separator + "mapDB" + File.separator + jobId, "Map.db");
        FileContentStreamInfoController.mapMapDb.put(jobId, mapDBHelper);
        htMap = mapDBHelper.htMap("FileParm", 24 * 60);
        htMapThread = mapDBHelper.htMap("FileThreadSign", 24 * 60);
        this.countDownLatch = countDownLatch;
    }

    @Override
    public void run() {
        try {
            Map<String, Object> jsonConf = (Map<String, Object>) json.get("sdm_receive_conf");
            String readType = null;
            if (null != jsonConf.get("read_type")) {
                readType = jsonConf.get("read_type").toString();
            }
            boolean readeOpinion;
            if (IsFlag.Fou == IsFlag.ofEnumByCode(readType)) {
                readeOpinion = false;
            } else {
                readeOpinion = true;
            }
            String file_handle = null;
            if (null != jsonConf.get("file_handle")) {
                file_handle = jsonConf.get("file_handle").toString();
            }
            FileDataValidator fileDataValidator = null;
            if (StringUtil.isNotBlank(file_handle)) {
                CusClassLoader classLoader = new CusClassLoader();
                Class<?> clazz = classLoader.getURLClassLoader().loadClass(file_handle);
                fileDataValidator = (FileDataValidator) clazz.newInstance();
            }
            String charset = null;
            if (null != jsonConf.get("code")) {
                charset = jsonConf.get("code").toString();
            }
            String readMode = null;
            if (null != jsonConf.get("read_mode")) {
                readMode = jsonConf.get("read_mode").toString();
            }
            Integer thread_num = null;
            if (null != jsonConf.get("thread_num")) {
                thread_num = Integer.parseInt(jsonConf.get("thread_num").toString());
            }
            executor = Executors.newFixedThreadPool(thread_num);
            FileContentStreamInfoController.mapExec.put(jobId, executor);
            String is_data_partition = null;
            if (null != jsonConf.get("is_data_partition")) {
                is_data_partition = jsonConf.get("is_data_partition").toString();
            }
            ProducerOperatorDir producerOperator = new ProducerOperatorDir();
            JobParamsEntity jobParams = producerOperator.getMapParam(is_data_partition, json, jobId);
            String filePath = null;
            if (null != jsonConf.get("ra_file_path")) {
                filePath = jsonConf.get("ra_file_path").toString();
            }
            String pathRename = filePath + File.separator + "rename";
            File fileRename = new File(pathRename);
            if (!fileRename.exists()) {
                if (!fileRename.mkdirs()) {
                    throw new BusinessException("创建目录失败：" + pathRename);
                }
            }
            String isObj = null;
            if (null != jsonConf.get("is_obj")) {
                isObj = jsonConf.get("is_obj").toString();
            }
            jobParams.setIsObj(isObj);
            String sdm_dat_delimiter = null;
            if (null != jsonConf.get("sdm_dat_delimiter")) {
                sdm_dat_delimiter = jsonConf.get("sdm_dat_delimiter").toString();
            }
            if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
                jobParams.setSdmDatelimiter(sdm_dat_delimiter);
            }
            htMapThread.clear();
            String matchRule = null;
            if (null != jsonConf.get("file_match_rule")) {
                matchRule = jsonConf.get("file_match_rule").toString();
            }
            final Pattern pattern;
            if (StringUtil.isBlank(matchRule)) {
                pattern = null;
            } else {
                pattern = Pattern.compile(matchRule);
            }
            FilenameFilter fileNameFilter = (dir, name) -> {
                if (pattern != null) {
                    Matcher matcher = pattern.matcher(name);
                    return matcher.matches() && !htMapThread.containsKey(name);
                } else {
                    return !htMapThread.containsKey(name);
                }
            };
            while (reading) {
                this.reading = readeOpinion;
                if (!FileContentStreamInfoController.mapStop.get(jobId)) {
                    break;
                }
                File root = new File(filePath);
                File[] files = root.listFiles(fileNameFilter);
                if (files != null && files.length > 0) {
                    for (File file : files) {
                        if (!FileContentStreamInfoController.mapStop.get(jobId)) {
                            break;
                        }
                        if (!file.isDirectory() && !htMapThread.containsKey(file.getName())) {
                            String fileName = file.getName();
                            if (!htMap.containsKey(fileName)) {
                                ReaderFileAvro readerFile = new ReaderFileAvro(mapDBHelper, htMap, htMapThread, readMode, jobParams, file, fileRename, charset, null, fileDataValidator);
                                executor.submit(readerFile);
                                htMapThread.put(fileName, "start");
                            } else if (!htMap.get(fileName).equals("all")) {
                                String position = null;
                                if (null != jsonConf.get("file_initposition")) {
                                    position = jsonConf.get("file_initposition").toString();
                                }
                                if (position.equals("0")) {
                                    ReaderFileAvro readerFile = new ReaderFileAvro(mapDBHelper, htMap, htMapThread, readMode, jobParams, file, fileRename, charset, null, fileDataValidator);
                                    executor.submit(readerFile);
                                    htMapThread.put(fileName, "start");
                                } else if (position.equals("-2")) {
                                    String beforeLine = htMap.get(fileName);
                                    ReaderFileAvro readerFile = new ReaderFileAvro(mapDBHelper, htMap, htMapThread, readMode, jobParams, file, fileRename, charset, beforeLine, fileDataValidator);
                                    executor.submit(readerFile);
                                    htMapThread.put(fileName, "start");
                                } else {
                                    logger.error("DirListenerReader--------------------------此采集方式只支持重新开始和从上次位置开始！！！");
                                }
                            } else {
                                try {
                                    File fileOld = new File((fileRename.getAbsolutePath() + File.separator + file.getName()));
                                    if (FileUtils.directoryContains(fileRename, fileOld)) {
                                        if (!fileOld.delete()) {
                                            throw new BusinessException("删除旧文件失败！！！");
                                        }
                                    }
                                    FileUtils.moveToDirectory(file, fileRename, true);
                                } catch (IOException e) {
                                    logger.error("文件移动失败！！！", e);
                                }
                            }
                        }
                    }
                } else {
                    try {
                        Thread.sleep(500);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
            while (!htMapThread.isEmpty()) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException ignored) {
                }
            }
            mapDBHelper.close();
            KafkaProducer<String, GenericRecord> producer = jobParams.getProducer();
            GenericRecord genericRecord = getParamGenericRecord2(jobParams.getListColumn(), jobParams.getSchema(), Constant.STREAM_HYREN_END, is_data_partition);
            KafkaProducerWorker kafkaProducerWorker = new KafkaProducerWorker();
            kafkaProducerWorker.sendToKafka(filePath, producer, genericRecord, jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync());
            producer.close();
            if (this.countDownLatch != null) {
                this.countDownLatch.countDown();
            }
        } catch (Exception e) {
            logger.error("DirListenerReader-----------------------文件流启动失败！！！", e);
            Thread.currentThread().interrupt();
            throw new BusinessException(e.getMessage());
        }
    }

    public GenericRecord getParamGenericRecord2(List<String> listColumn, Schema customSchema, String message, String is_data_partition) {
        GenericRecord genericRecord = new GenericData.Record(customSchema);
        if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
            for (String column : listColumn) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                    genericRecord.put(column.split("`")[0], message);
                }
            }
        } else {
            genericRecord.put("line", message);
        }
        return genericRecord;
    }
}
