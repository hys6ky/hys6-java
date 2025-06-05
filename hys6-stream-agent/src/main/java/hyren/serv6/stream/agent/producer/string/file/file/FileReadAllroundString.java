package hyren.serv6.stream.agent.producer.string.file.file;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.stream.CusClassLoader;
import hyren.serv6.commons.utils.stream.CustomerPartition;
import hyren.serv6.commons.utils.stream.KafkaProducerError;
import hyren.serv6.stream.agent.producer.commons.*;
import hyren.serv6.stream.agent.realtimecollection.filecontentstream.FileContentStreamInfoController;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.*;

public class FileReadAllroundString implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private static final KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public static final Map<String, Thread> mapJob = new HashMap<>();

    public String jobId;

    public static final long READ_POSITION_HEAD = 0L;

    public static final long READ_POSITION_TAIL = -1L;

    public static final long READ_POSITION_LATEST = -2L;

    public static final long READ_POSITION_ASSIGN = 1L;

    private ExecutorService executor;

    private ArtisanRunnable runner;

    private Future<?> runnerFuture;

    private String sourceFilepath;

    private long position;

    private String sourceFileCharset;

    private FileDataValidator filedataValidator;

    private String file_read_num;

    private JobParamsEntity jobParams;

    private boolean readeOpinion;

    private String is_data_partition;

    private Map<String, Object> json;

    private CountDownLatch countDownLatch;

    private String read_mode;

    private String is_obj;

    public FileReadAllroundString(String jobId, Map<String, Object> json, CountDownLatch countDownLatch) {
        this.jobId = jobId;
        this.json = json;
        this.countDownLatch = countDownLatch;
        executor = FileContentStreamInfoController.mapExec.get(jobId);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            quit();
            logger.error("FileReadAllroundString-------------------FileReadAllround shutdown hook down." + System.currentTimeMillis());
        }));
    }

    public void config(String is_data_partition, boolean readeOpinion, String filepath, long position, String sourceFileCharset, FileDataValidator filedataValidator, String file_read_num, JobParamsEntity jobParams, String read_mode, String is_obj) {
        this.is_data_partition = is_data_partition;
        this.sourceFilepath = filepath;
        this.position = position;
        this.sourceFileCharset = sourceFileCharset;
        this.filedataValidator = filedataValidator;
        this.jobParams = jobParams;
        this.file_read_num = file_read_num;
        this.readeOpinion = readeOpinion;
        this.read_mode = read_mode;
        this.is_obj = is_obj;
    }

    public void startup() {
        logger.info("FileReadAllroundString------------------------FileReadAllround startup.");
        executor = Executors.newSingleThreadExecutor();
        try {
            runner = new ArtisanRunnable(this.is_data_partition, this.readeOpinion, this.sourceFilepath, this.position, this.sourceFileCharset, this.filedataValidator, this.file_read_num, this.jobParams, this.countDownLatch, this.read_mode, this.is_obj);
            Thread thread = mapJob.get(jobId);
            if (thread != null) {
                thread.interrupt();
                runnerFuture = executor.submit(runner);
            } else {
                runnerFuture = executor.submit(runner);
            }
        } catch (Exception e) {
            logger.error("FileReadAllroundString------------------startup failed!", e);
            quit();
            throw new BusinessException(e.getMessage());
        }
    }

    public void quit() {
        if (runner != null) {
            runner.kill();
        }
        logger.info("FileReadAllroundString-------------------FileReadAllround quit step 1 runner killed.");
        if (runnerFuture != null) {
            runnerFuture.cancel(true);
        }
        logger.info("FileReadAllroundString-------------------FileReadAllround quit step 2 runnerFuture canceled.");
        executor.shutdown();
        logger.info("FileReadAllroundString--------------------FileReadAllround quit step 3 executor shutdown.");
        while (!executor.isTerminated()) {
            try {
                executor.awaitTermination(500, TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void run() {
        try {
            Map<String, Object> jsonParams = (Map<String, Object>) json.get("sdm_receive_conf");
            String readType = null;
            if (null != jsonParams.get("read_type")) {
                readType = jsonParams.get("read_type").toString();
            }
            this.readeOpinion = IsFlag.Fou != IsFlag.ofEnumByCode(readType);
            String sdm_receive_id = null;
            if (null != jsonParams.get("sdm_receive_id")) {
                sdm_receive_id = jsonParams.get("sdm_receive_id").toString();
            }
            this.jobId = sdm_receive_id;
            ProducerOperatorFileString producerOperator = new ProducerOperatorFileString();
            JobParamsEntity jobParams = producerOperator.getMapParam(json, jobId);
            String sourceFilepath = null;
            if (null != jsonParams.get("ra_file_path")) {
                sourceFilepath = jsonParams.get("ra_file_path").toString();
            }
            String fileValidatorClassname = null;
            if (null != jsonParams.get("file_handle")) {
                fileValidatorClassname = jsonParams.get("file_handle").toString();
            }
            FileDataValidator filedataValidator = null;
            if (!StringUtil.isBlank(fileValidatorClassname)) {
                CusClassLoader classLoader = new CusClassLoader();
                Class<?> clazz = classLoader.getURLClassLoader().loadClass(fileValidatorClassname);
                filedataValidator = (FileDataValidator) clazz.newInstance();
            }
            String position = null;
            if (null != jsonParams.get("file_initposition")) {
                position = jsonParams.get("file_initposition").toString();
            }
            String sourceFileCharset = null;
            if (null != jsonParams.get("code")) {
                sourceFileCharset = jsonParams.get("code").toString();
            }
            String read_mode = null;
            if (null != jsonParams.get("read_mode")) {
                read_mode = jsonParams.get("read_mode").toString();
            }
            String is_obj = null;
            if (null != jsonParams.get("is_obj")) {
                is_obj = jsonParams.get("is_obj").toString();
            }
            String is_data_partition = null;
            if (null != jsonParams.get("is_data_partition")) {
                is_data_partition = jsonParams.get("is_data_partition").toString();
            }
            String sdm_dat_delimiter = null;
            if (null != jsonParams.get("sdm_dat_delimiter")) {
                sdm_dat_delimiter = jsonParams.get("sdm_dat_delimiter").toString();
            }
            if (Objects.isNull(jsonParams.get("file_read_num"))) {
                file_read_num = "1";
                logger.info("file_read_num is null , set default num : 1");
            } else {
                file_read_num = jsonParams.get("file_read_num").toString();
            }
            if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
                jobParams.setSdmDatelimiter(sdm_dat_delimiter);
                if (Objects.isNull(jsonParams.get("sdm_dat_delimiter"))) {
                    jobParams.setSdmDatelimiter("\\u7c\\u7c");
                    logger.info("sdm_dat_delimiter is null , set default num : \\u7c\\u7c");
                } else {
                    jobParams.setSdmDatelimiter(jsonParams.get("sdm_dat_delimiter").toString());
                }
            }
            config(is_data_partition, readeOpinion, sourceFilepath, Long.parseLong(position), sourceFileCharset, filedataValidator, file_read_num, jobParams, read_mode, is_obj);
            startup();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            quit();
        }
    }

    private class ArtisanRunnable implements Runnable {

        private String filepath;

        private File file;

        private long readSeekPos;

        private String charset;

        private String file_read_num;

        private FileDataValidator filedataValidator;

        private KafkaProducer<String, String> producer;

        private String topic;

        private CustomerPartition cp;

        private String bootstrapServers;

        private String jobId;

        private boolean readeOpinion;

        private List<String> listColumn;

        private boolean reading;

        private String is_data_partition;

        private String sync;

        private CountDownLatch countDownLatch;

        private String read_mode;

        private String is_obj;

        public ArtisanRunnable(String is_data_partition, boolean readeOpinion, String filepath, long readSeekPos, String charset, FileDataValidator filedataValidator, String file_read_num, JobParamsEntity jobParams, CountDownLatch countDownLatch, String read_mode, String is_obj) throws Exception {
            this.filepath = filepath;
            this.file = new File(filepath);
            this.readSeekPos = readSeekPos;
            this.charset = charset;
            this.reading = true;
            this.filedataValidator = filedataValidator;
            this.file_read_num = file_read_num;
            this.producer = jobParams.getProducerString();
            this.topic = jobParams.getTopic();
            this.cp = jobParams.getCustomerPartition();
            this.bootstrapServers = jobParams.getBootstrapServers();
            this.jobId = jobParams.getJobId();
            this.readeOpinion = readeOpinion;
            this.listColumn = jobParams.getListColumn();
            this.is_data_partition = is_data_partition;
            this.sync = jobParams.getSync();
            this.countDownLatch = countDownLatch;
            this.read_mode = read_mode;
            this.is_obj = is_obj;
        }

        @Override
        public void run() {
            mapJob.put(jobId, Thread.currentThread());
            if (!this.file.exists()) {
                logger.error("FileReadAllroundString--------------------file [{}] is not exist! " + filepath);
                return;
            }
            long filePointer = 0;
            long lineNum = 0;
            if (this.readSeekPos == READ_POSITION_TAIL) {
                filePointer = this.file.length();
            } else if (this.readSeekPos == READ_POSITION_HEAD)
                filePointer = 0;
            else if (this.readSeekPos == READ_POSITION_LATEST) {
                try (RandomAccessFile rafFilePointer = new RandomAccessFile(new File(filepath + ".rdp"), "r")) {
                    rafFilePointer.seek(0);
                    filePointer = rafFilePointer.readLong();
                } catch (Exception e) {
                    if (e instanceof FileNotFoundException)
                        filePointer = 0;
                    else {
                        logger.error("FileReadAllroundString-------------read rdp file [" + this.filepath + "] failed!", e);
                        return;
                    }
                }
            } else if (this.readSeekPos == READ_POSITION_ASSIGN)
                lineNum = Long.parseLong(this.file_read_num);
            else {
                logger.error("FileReadAllroundString----------------file read position[{}] is wrong! " + this.readSeekPos);
                return;
            }
            RandomAccessFile rafSourceFile = null;
            KafkaProducerWorker kafkaProducerWorkerString = new KafkaProducerWorker();
            try (RandomAccessFile rafFilePointer = new RandomAccessFile(new File(filepath + ".rdp"), "rw")) {
                rafSourceFile = new RandomAccessFile(file, "r");
                StringBuilder lineBuffer = new StringBuilder(1024);
                GetFileParams getFileParams = new GetFileParams();
                Map<String, Object> json = getFileParams.getParmJson(listColumn, file);
                if (lineNum > 0) {
                    for (int i = 1; i < lineNum; i++) {
                        rafSourceFile.readLine();
                    }
                } else {
                    rafSourceFile.seek(filePointer);
                }
                while (this.reading) {
                    this.reading = readeOpinion;
                    long curfileLength = file.length();
                    if (curfileLength < filePointer || (curfileLength == 0 && filePointer == 0)) {
                        if (rafSourceFile != null) {
                            rafSourceFile.close();
                        }
                        rafSourceFile = new RandomAccessFile(file, "r");
                        filePointer = 0;
                        rafFilePointer.seek(0);
                        rafFilePointer.writeLong(0L);
                    }
                    while (true) {
                        String curLineString = rafSourceFile.readLine();
                        if (curLineString == null) {
                            if (lineBuffer.length() > 0) {
                                json = getFileParams.getRealJson(jobParams, lineBuffer, charset, listColumn, json, read_mode);
                                if (kafkaProducerWorkerString.sendToKafka(filepath, producer, json.toString(), topic, cp, bootstrapServers, sync)) {
                                    filePointer = saveReadingPointer(rafSourceFile, rafFilePointer);
                                    lineBuffer.delete(0, lineBuffer.length());
                                } else {
                                    logger.error("FileReadAllroundString------------数据发送失败，失败数据为：" + lineBuffer.toString());
                                    KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, json.toString());
                                    break;
                                }
                            }
                            TimeUnit.MILLISECONDS.sleep(500);
                            break;
                        }
                        if (isNewLine(curLineString)) {
                            if (lineBuffer.length() < 1) {
                                lineBuffer.append(curLineString);
                            } else {
                                json = getFileParams.getRealJson(jobParams, lineBuffer, charset, listColumn, json, read_mode);
                                if (kafkaProducerWorkerString.sendToKafka(filepath, producer, json.toString(), topic, cp, bootstrapServers, sync)) {
                                    filePointer = saveReadingPointer(rafSourceFile, rafFilePointer, curLineString);
                                    lineBuffer.delete(0, lineBuffer.length());
                                    lineBuffer.append(curLineString);
                                } else {
                                    logger.error("FileReadAllroundString---------------数据发送失败！！！");
                                    break;
                                }
                            }
                        } else {
                            lineBuffer.append('\n').append(curLineString);
                        }
                    }
                }
                json = getParmJson2(listColumn, Constant.STREAM_HYREN_END);
                kafkaProducerWorkerString.sendToKafka(filepath, producer, json.toString(), topic, cp, bootstrapServers, sync);
                if (this.countDownLatch != null) {
                    this.countDownLatch.countDown();
                }
                if (producer != null) {
                    producer.close();
                }
            } catch (Exception e) {
                if (e instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                } else {
                    logger.error(e);
                    throw new BusinessException(e.getMessage());
                }
            } finally {
                try {
                    if (rafSourceFile != null) {
                        rafSourceFile.close();
                    }
                } catch (Exception e) {
                }
            }
        }

        private long saveReadingPointer(RandomAccessFile rafSource, RandomAccessFile rafPointer) throws IOException {
            long pointer = rafSource.getFilePointer();
            rafPointer.seek(0);
            rafPointer.writeLong(pointer);
            return pointer;
        }

        private long saveReadingPointer(RandomAccessFile rafSource, RandomAccessFile rafPointer, String curLineString) throws IOException {
            long pointer = rafSource.getFilePointer();
            pointer = pointer - curLineString.length() - 2;
            rafPointer.seek(0);
            rafPointer.writeLong(pointer);
            return pointer;
        }

        private boolean isNewLine(String lineText) {
            if (filedataValidator != null) {
                return this.filedataValidator.isNewLine(lineText);
            } else {
                return true;
            }
        }

        @SuppressWarnings("unused")
        private boolean isSkipLine(String lineText) {
            return this.filedataValidator.isSkipLine(lineText);
        }

        public void kill() {
            this.reading = false;
            synchronized (this.getClass()) {
            }
        }

        private Map<String, Object> getParmJson2(List<String> listColumn, String message) {
            Map<String, Object> json = new HashMap<>();
            if ("1".equals(read_mode)) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            json.put(column.split("`")[0], message);
                        }
                    }
                } else {
                    json.put("line", message);
                }
            } else {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(is_obj)) {
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            json.put(column.split("`")[0], message);
                        }
                    }
                } else {
                    json.put("line", message);
                }
            }
            return json;
        }
    }
}
