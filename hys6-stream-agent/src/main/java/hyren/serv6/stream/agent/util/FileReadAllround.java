package hyren.serv6.stream.agent.util;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.stream.CusClassLoader;
import hyren.serv6.commons.utils.stream.CustomerPartition;
import hyren.serv6.commons.utils.stream.KafkaProducerError;
import hyren.serv6.stream.agent.producer.avro.file.file.ProducerOperatorFile;
import hyren.serv6.stream.agent.producer.commons.*;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class FileReadAllround implements Runnable {

    private static final Logger logger = LogManager.getLogger();

    private static final KafkaProducerError KAFKA_PRODUCER_ERROR = new KafkaProducerError();

    public static final long READ_POSITION_HEAD = 0L;

    public static final long READ_POSITION_TAIL = -1L;

    public static final long READ_POSITION_LATEST = -2L;

    public static final long READ_POSITION_ASSIGN = 1L;

    public String jobId;

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

    public FileReadAllround(String jobId, Map<String, Object> json, CountDownLatch countDownLatch) {
        this.jobId = jobId;
        this.json = json;
        this.countDownLatch = countDownLatch;
        executor = KafkaStartParam.mapExec.get(jobId);
        Runtime.getRuntime().addShutdownHook(new Thread() {

            @Override
            public void run() {
                quit();
            }
        });
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
        executor = Executors.newSingleThreadExecutor();
        try {
            runner = new ArtisanRunnable(this.is_data_partition, this.readeOpinion, this.sourceFilepath, this.position, this.sourceFileCharset, this.filedataValidator, this.file_read_num, this.jobParams, this.countDownLatch, this.read_mode, this.is_obj);
            Thread thread = KafkaStartParam.mapJob.get(jobId);
            if (thread != null) {
                thread.interrupt();
                runnerFuture = executor.submit(runner);
            } else {
                runnerFuture = executor.submit(runner);
            }
        } catch (Exception e) {
            quit();
        }
    }

    public void quit() {
        if (runner != null) {
            runner.kill();
        }
        if (runnerFuture != null) {
            runnerFuture.cancel(true);
        }
        executor.shutdown();
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
            ProducerOperatorFile producerOperator = new ProducerOperatorFile();
            String is_data_partition = null;
            if (null != jsonParams.get("is_data_partition")) {
                is_data_partition = jsonParams.get("is_data_partition").toString();
            }
            String sdm_dat_delimiter = null;
            if (null != jsonParams.get("sdm_dat_delimiter")) {
                sdm_dat_delimiter = jsonParams.get("sdm_dat_delimiter").toString();
            }
            JobParamsEntity jobParams = producerOperator.getMapParam(is_data_partition, json, jobId);
            if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
                jobParams.setSdmDatelimiter(sdm_dat_delimiter);
            }
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
            String file_read_num = null;
            if (null != jsonParams.get("file_read_num")) {
                file_read_num = jsonParams.get("file_read_num").toString();
            }
            String read_mode = null;
            if (null != jsonParams.get("read_mode")) {
                read_mode = jsonParams.get("read_mode").toString();
            }
            String is_obj = null;
            if (null != jsonParams.get("is_obj")) {
                is_obj = jsonParams.get("is_obj").toString();
            }
            config(is_data_partition, readeOpinion, sourceFilepath, Long.parseLong(position), sourceFileCharset, filedataValidator, file_read_num, jobParams, read_mode, is_obj);
            startup();
        } catch (Exception e) {
            quit();
            throw new BusinessException("失败");
        }
    }

    private class ArtisanRunnable implements Runnable {

        private String filepath;

        private File file;

        private long readSeekPos;

        private String charset;

        private String file_read_num;

        private FileDataValidator filedataValidator;

        private KafkaProducer<String, GenericRecord> producer;

        private String topic;

        private CustomerPartition cp;

        private String bootstrapServers;

        private String jobId;

        private boolean readeOpinion;

        private List<String> listColumn;

        private boolean reading;

        private String is_data_partition;

        private Schema customSchema;

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
            this.producer = jobParams.getProducer();
            this.topic = jobParams.getTopic();
            this.cp = jobParams.getCustomerPartition();
            this.bootstrapServers = jobParams.getBootstrapServers();
            this.jobId = jobParams.getJobId();
            this.readeOpinion = readeOpinion;
            this.listColumn = jobParams.getListColumn();
            this.is_data_partition = is_data_partition;
            this.customSchema = jobParams.getSchema();
            this.sync = jobParams.getSync();
            this.countDownLatch = countDownLatch;
            this.read_mode = read_mode;
            this.is_obj = is_obj;
        }

        @Override
        public void run() {
            KafkaStartParam.mapJob.put(jobId, Thread.currentThread());
            if (!this.file.exists()) {
                return;
            }
            logger.debug("FileReadAllround---------------------file length is " + this.file.length());
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
                        return;
                    }
                }
            } else if (this.readSeekPos == READ_POSITION_ASSIGN)
                lineNum = Long.parseLong(this.file_read_num);
            else {
                return;
            }
            RandomAccessFile rafSourceFile = null;
            KafkaProducerWorker kafkaProducerWorkerAvro = new KafkaProducerWorker();
            try (RandomAccessFile rafFilePointer = new RandomAccessFile(new File(filepath + ".rdp"), "rw")) {
                rafSourceFile = new RandomAccessFile(file, "r");
                StringBuilder lineBuffer = new StringBuilder(1024);
                GenericRecord genericRecord = new GenericData.Record(jobParams.getSchema());
                GetFileParams getFileParams = new GetFileParams();
                genericRecord = getFileParams.getParmGenericRecordAvro(listColumn, file, genericRecord);
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
                                genericRecord = getFileParams.getRealGenericRecordAvro(jobParams, lineBuffer, listColumn, genericRecord, read_mode);
                                if (kafkaProducerWorkerAvro.sendToKafka(filepath, producer, genericRecord, topic, cp, bootstrapServers, sync)) {
                                    filePointer = saveReadingPointer(rafSourceFile, rafFilePointer);
                                    lineBuffer.delete(0, lineBuffer.length());
                                } else {
                                    KAFKA_PRODUCER_ERROR.sendToKafka(bootstrapServers, topic, genericRecord.toString());
                                    break;
                                }
                            }
                            TimeUnit.MILLISECONDS.sleep(500);
                            break;
                        }
                        if (isNewLine(curLineString, filedataValidator)) {
                            if (lineBuffer.length() < 1) {
                                lineBuffer.append(curLineString);
                            } else {
                                genericRecord = getFileParams.getRealGenericRecordAvro(jobParams, lineBuffer, listColumn, genericRecord, read_mode);
                                if (kafkaProducerWorkerAvro.sendToKafka(filepath, producer, genericRecord, topic, cp, bootstrapServers, sync)) {
                                    filePointer = saveReadingPointer(rafSourceFile, rafFilePointer);
                                    lineBuffer.delete(0, lineBuffer.length());
                                    lineBuffer.append(curLineString);
                                } else {
                                    break;
                                }
                            }
                        } else {
                            lineBuffer.append('\n').append(curLineString);
                        }
                    }
                }
                genericRecord = getParmGenericRecord2(listColumn, file, customSchema, "hyren_end_end_end");
                kafkaProducerWorkerAvro.sendToKafka(filepath, producer, genericRecord, topic, cp, bootstrapServers, sync);
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
                    e.printStackTrace();
                    throw new BusinessException("读取文件失败");
                }
            } finally {
                try {
                    if (rafSourceFile != null) {
                        rafSourceFile.close();
                        rafSourceFile = null;
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

        private boolean isNewLine(String lineText, FileDataValidator filedataValidator) {
            if (filedataValidator != null) {
                return filedataValidator.isNewLine(lineText);
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

        public GenericRecord getParmGenericRecord2(List<String> listColumn, File file, Schema customSchema, String message) {
            GenericRecord genericRecord = new GenericData.Record(customSchema);
            if ("1".equals(read_mode)) {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(is_data_partition)) {
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            genericRecord.put(column.split("`")[0], message);
                        }
                    }
                } else {
                    genericRecord.put("line", message);
                }
            } else {
                if (IsFlag.Shi == IsFlag.ofEnumByCode(is_obj)) {
                    for (String column : listColumn) {
                        if (IsFlag.Shi == IsFlag.ofEnumByCode(column.split("`")[1])) {
                            genericRecord.put(column.split("`")[0], message);
                        }
                    }
                } else {
                    genericRecord.put("line", message);
                }
            }
            return genericRecord;
        }
    }
}
