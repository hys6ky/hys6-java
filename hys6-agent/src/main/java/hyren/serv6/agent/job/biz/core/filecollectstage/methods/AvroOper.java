package hyren.serv6.agent.job.biz.core.filecollectstage.methods;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.trans.biz.unstructuredfilecollect.FileCollectJobService;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.utils.TextUtil;
import hyren.serv6.commons.utils.agent.bean.FileCollectParamBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.agent.constant.PropertyParaUtil;
import hyren.serv6.commons.utils.fileutil.read.ReadFileUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentMap;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/11/14 15:32")
public class AvroOper {

    private static final String SCHEMA_JSON = "{\"type\": \"record\",\"name\": \"SmallFilesTest\", " + "\"fields\": [" + "{\"name\":\"" + "file_name" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_scr_path" + "\", \"type\":\"string\"}," + "{\"name\":\"" + "file_size" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_time" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_summary" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_text" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_md5" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_avro_path" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_avro_block" + "\",\"type\":\"long\"}," + "{\"name\":\"" + "is_big_file" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "file_contents" + "\",\"type\":\"bytes\"}," + "{\"name\":\"" + "uuid" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "is_increasement" + "\",\"type\":\"string\"}," + "{\"name\":\"" + "is_cache" + "\",\"type\":\"string\"}" + "]}";

    private static final Schema SCHEMA = new Schema.Parser().parse(SCHEMA_JSON);

    private final FileCollectParamBean fileCollectParamBean;

    private final CollectionWatcher collectionWatcher;

    private static final String BIGFILENAME = "bigFiles";

    private final String fileCollectHdfsPath;

    private final String bigFileCollectHdfsPath;

    private final ArrayBlockingQueue<String> queue;

    private final ConcurrentMap<String, String> fileNameHTreeMap;

    public static final long LASTELEMENT = 0L;

    public AvroOper(FileCollectParamBean fileCollectParamBean, ConcurrentMap<String, String> fileNameHTreeMap) {
        this.fileCollectParamBean = fileCollectParamBean;
        this.fileNameHTreeMap = fileNameHTreeMap;
        collectionWatcher = new CollectionWatcher(fileCollectParamBean);
        fileCollectHdfsPath = FileNameUtils.normalize(JobConstant.PREFIX + File.separator + DataSourceType.DCL.getCode() + File.separator + fileCollectParamBean.getFcs_id() + File.separator + fileCollectParamBean.getFile_source_id() + File.separator, true);
        bigFileCollectHdfsPath = FileNameUtils.normalize(fileCollectHdfsPath + File.separator + BIGFILENAME + File.separator, true);
        queue = FileCollectJobService.mapQueue.get(fileCollectParamBean.getFile_source_id());
    }

    public CollectionWatcher getCollectionWatcher() {
        return collectionWatcher;
    }

    private long putOneFile2Avro(String filePath, long avroFileTotalSize, String avroHdfsPath, DataFileWriter<Object> writer, boolean isSelect) throws IOException {
        GenericRecord record = new GenericData.Record(SCHEMA);
        try {
            long syncBlock = writer.sync();
            File file = new File(filePath);
            String fileName = file.getName();
            record.put("is_cache", "");
            if (isSelect) {
                record.put("uuid", JsonUtil.toObject(JsonUtil.toJson(fileNameHTreeMap.get(filePath)), new TypeReference<Map<String, Object>>() {
                }).get("uuid"));
                record.put("is_increasement", IsFlag.Shi.getCode());
            } else {
                record.put("uuid", UUID.randomUUID().toString());
                record.put("is_increasement", IsFlag.Fou.getCode());
            }
            record.put("file_name", fileName);
            record.put("file_scr_path", filePath);
            record.put("file_size", String.valueOf(file.length()));
            record.put("file_time", String.valueOf(file.lastModified()));
            if (JobConstant.FILECHANGESTYPEMD5) {
                record.put("file_md5", MD5Util.md5File(filePath));
            } else {
                record.put("file_md5", String.valueOf(new File(filePath).lastModified()));
            }
            record.put("file_avro_block", syncBlock);
            if (file.length() > JobConstant.THRESHOLD_FILE_SIZE) {
                String bigFileHdfs = bigFileCollectHdfsPath + fileName;
                record.put("file_avro_path", bigFileHdfs);
                record.put("is_big_file", IsFlag.Shi.getCode());
                record.put("file_contents", ByteBuffer.wrap(bigFileHdfs.getBytes()));
                record.put("file_summary", "");
                record.put("file_text", "");
                avroFileTotalSize++;
                putIntoQueue(file.getAbsolutePath(), fileCollectParamBean, false, true);
            } else {
                record.put("file_avro_path", fileCollectHdfsPath + FileNameUtils.getName(avroHdfsPath));
                record.put("is_big_file", IsFlag.Fou.getCode());
                record.put("file_contents", ByteBuffer.wrap(FileUtils.readFileToByteArray(file)));
                String text = ReadFileUtil.file2String(file);
                try {
                    String summary = TextUtil.etractSummary(text, Integer.parseInt(PropertyParaUtil.getString("summary_volumn", "3")));
                    record.put("file_summary", TextUtil.normalizeSummary(summary));
                } catch (OutOfMemoryError e) {
                    throw new BusinessException("提取文本摘要发生异常!" + e);
                }
                text = TextUtil.normalizeText(text);
                record.put("file_text", text);
                avroFileTotalSize += file.length();
            }
            writer.append(record);
            return avroFileTotalSize;
        } catch (IOException e) {
            log.error("Failed to putOneFile2Avro ... " + filePath, e);
            throw e;
        }
    }

    public void putAllFiles2Avro(List<String> files, boolean isSelect, boolean isLast) {
        String unLoadPath = fileCollectParamBean.getUnLoadPath();
        File file2 = new File(unLoadPath);
        if (!file2.exists()) {
            boolean mkdirs = file2.mkdirs();
            if (!mkdirs) {
                throw new AppSystemException("创建文件夹" + file2.getAbsolutePath() + "失败");
            }
        }
        DataFileWriter<Object> writer = null;
        OutputStream outputStream = null;
        String avroFileAbsolutionPath = "";
        long avroFileTotalSize = 0L;
        try {
            if (files.size() > 0) {
                for (String filePath : files) {
                    if (avroFileTotalSize > JobConstant.SINGLE_AVRO_SIZE || avroFileTotalSize == 0L) {
                        avroFileTotalSize = 0L;
                        if (writer != null) {
                            writer.close();
                        }
                        if (outputStream != null) {
                            outputStream.close();
                        }
                        if (!StringUtil.isBlank(avroFileAbsolutionPath)) {
                            putIntoQueue(avroFileAbsolutionPath, fileCollectParamBean, false, false);
                        }
                        avroFileAbsolutionPath = unLoadPath + "avro_" + UUID.randomUUID();
                        log.info("Ready to generate avro file: " + avroFileAbsolutionPath);
                        outputStream = Files.newOutputStream(Paths.get(avroFileAbsolutionPath));
                        writer = new DataFileWriter<>(new GenericDatumWriter<>());
                        writer.setCodec(CodecFactory.snappyCodec());
                        writer.create(SCHEMA, outputStream);
                    }
                    avroFileTotalSize = putOneFile2Avro(filePath, avroFileTotalSize, avroFileAbsolutionPath, writer, isSelect);
                }
                putIntoQueue(avroFileAbsolutionPath, fileCollectParamBean, isLast, false);
            }
        } catch (IOException e) {
            log.error("Failed to create Avro File...", e);
            throw new AppSystemException(e.getMessage());
        } finally {
            try {
                if (writer != null) {
                    writer.close();
                }
                if (outputStream != null) {
                    outputStream.close();
                }
            } catch (Exception e) {
                log.error("关闭流失败", e);
            }
        }
    }

    private void putIntoQueue(String avroFileAbsolutionPath, FileCollectParamBean fileCollectParamBean, boolean isLastElement, boolean isBigFile) {
        try {
            Map<String, Object> queueJb = new HashMap<>();
            queueJb.put("source_id", fileCollectParamBean.getSource_id());
            queueJb.put("agent_id", fileCollectParamBean.getAgent_id());
            queueJb.put("sys_date", fileCollectParamBean.getSysDate());
            queueJb.put("collect_set_id", fileCollectParamBean.getFcs_id());
            queueJb.put("avroFileAbsolutionPath", avroFileAbsolutionPath);
            queueJb.put("job_rs_id", collectionWatcher.getJob_rs_id());
            if (isBigFile) {
                queueJb.put("isBigFile", IsFlag.Shi.getCode());
                queueJb.put("fileCollectHdfsPath", bigFileCollectHdfsPath);
            } else {
                queueJb.put("isBigFile", IsFlag.Fou.getCode());
                queueJb.put("fileCollectHdfsPath", fileCollectHdfsPath);
            }
            if (isLastElement) {
                queueJb.put("watcher_id", LASTELEMENT);
            } else {
                queueJb.put("watcher_id", System.currentTimeMillis());
            }
            String queueJbString = JsonUtil.toJson(queueJb);
            log.info("Put queueJbString: " + queueJbString + " into queue!");
            queue.put(queueJbString);
        } catch (Exception e) {
            log.error("Failed to upload the avro file or put information into queue ...", e);
            throw new AppSystemException(e.getMessage());
        }
    }
}
