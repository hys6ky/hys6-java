package hyren.serv6.stream.agent.producer.string.file.dirString;

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
import org.apache.commons.io.FileUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DirListenerReaderString implements Runnable {

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

    public DirListenerReaderString(String jobId, Map<String, Object> json, CountDownLatch countDownLatch) {
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
        this.mapDBHelper = new MapDBHelper(folder + File.separator + "mapDB" + File.separator + jobId, "Map.db");
        FileContentStreamInfoController.mapMapDb.put(jobId, mapDBHelper);
        this.htMap = mapDBHelper.htMap("FileParm", 24 * 60);
        this.htMapThread = mapDBHelper.htMap("FileThreadSign", 24 * 60);
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
            readeOpinion = IsFlag.Fou != IsFlag.ofEnumByCode(readType);
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
            String thread_num = null;
            if (null != jsonConf.get("thread_num")) {
                thread_num = jsonConf.get("thread_num").toString();
            }
            this.executor = Executors.newFixedThreadPool(Integer.parseInt(thread_num));
            FileContentStreamInfoController.mapExec.put(jobId, executor);
            ProducerOperatorDirString producerOperator = new ProducerOperatorDirString();
            JobParamsEntity jobParams = producerOperator.getMapParam(json, jobId);
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
            String isDataPartition = null;
            if (null != jsonConf.get("is_data_partition")) {
                isDataPartition = jsonConf.get("is_data_partition").toString();
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
            if (IsFlag.Shi == IsFlag.ofEnumByCode(isDataPartition)) {
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
            while (this.reading) {
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
                                ReaderFileString readerFile = new ReaderFileString(mapDBHelper, htMap, htMapThread, readMode, jobParams, file, fileRename, charset, null, fileDataValidator);
                                executor.submit(readerFile);
                                htMapThread.put(fileName, "start");
                            } else if (!htMap.get(fileName).equals("all")) {
                                String position = null;
                                if (null != jsonConf.get("file_initposition")) {
                                    position = jsonConf.get("file_initposition").toString();
                                }
                                if (position.equals("0")) {
                                    ReaderFileString readerFile = new ReaderFileString(mapDBHelper, htMap, htMapThread, readMode, jobParams, file, fileRename, charset, null, fileDataValidator);
                                    executor.submit(readerFile);
                                    htMapThread.put(fileName, "start");
                                } else if (position.equals("-2")) {
                                    String beforeLine = htMap.get(fileName);
                                    ReaderFileString readerFile = new ReaderFileString(mapDBHelper, htMap, htMapThread, readMode, jobParams, file, fileRename, charset, beforeLine, fileDataValidator);
                                    executor.submit(readerFile);
                                    htMapThread.put(fileName, "start");
                                } else {
                                    logger.error("DirListenerReaderString----------------------此采集方式只支持重新开始和从上次位置开始！！！");
                                }
                            } else {
                                try {
                                    File fileOld = new File((fileRename.getAbsolutePath() + File.separator + file.getName()));
                                    if (FileUtils.directoryContains(fileRename, fileOld)) {
                                        if (!fileOld.delete()) {
                                            throw new BusinessException("DirListenerReaderString--------------------删除旧文件失败！！" + fileOld.getPath());
                                        }
                                    }
                                    FileUtils.moveToDirectory(file, fileRename, true);
                                } catch (IOException e) {
                                    logger.error("DirListenerReaderString--------------------文件移动失败！！！", e);
                                    e.printStackTrace();
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
            KafkaProducer<String, String> producer = jobParams.getProducerString();
            Map<String, Object> json = getParamJson2(jobParams.getListColumn(), isDataPartition);
            KafkaProducerWorker kafkaProducerWorker = new KafkaProducerWorker();
            kafkaProducerWorker.sendToKafka(filePath, producer, json.toString(), jobParams.getTopic(), jobParams.getCustomerPartition(), jobParams.getBootstrapServers(), jobParams.getSync());
            producer.close();
            if (this.countDownLatch != null) {
                this.countDownLatch.countDown();
            }
        } catch (Exception e) {
            logger.error("DirListenerReaderString-------------------文件流启动失败！！！", e);
            throw new BusinessException("DirListenerReaderString---------------------文件流启动失败:" + e.getMessage());
        }
    }

    private Map<String, Object> getParamJson2(List<String> listColumn, String is_data_partition) {
        Map<String, Object> json = new HashMap<>();
        if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
            for (String column : listColumn) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                    json.put(column.split("`")[0], Constant.STREAM_HYREN_END);
                }
            }
        } else {
            json.put("line", Constant.STREAM_HYREN_END);
        }
        return json;
    }
}
