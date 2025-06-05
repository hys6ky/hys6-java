package hyren.serv6.hadoop.commons.ocr;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.solr.ISolrOperator;
import hyren.serv6.commons.solr.factory.SolrFactory;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.commons.ocr.PictureTextExtract;
import hyren.serv6.commons.utils.TextUtil;
import hyren.serv6.commons.utils.constant.CommonVariables;
import lombok.extern.slf4j.Slf4j;
import net.sourceforge.tess4j.ITesseract;
import net.sourceforge.tess4j.Tesseract;
import org.apache.avro.Schema;
import org.apache.avro.file.CodecFactory;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
public class OcrExtractText implements Closeable {

    private static final List<String> OCR_EXPECTED_EXTENSION_S = Arrays.asList("pdf", "bmp", "png", "tif", "jpg", "");

    public static Schema SCHEMA;

    private final ExecutorService fixedThreadPool = Executors.newFixedThreadPool(CommonVariables.OCR_THREAD_POOL);

    private static HdfsOperator hdfsOperator;

    private final String avroParentDir;

    static {
        try {
            Map<String, Object> SCHEMA_JSON = new HashMap<>();
            SCHEMA_JSON.put("type", "record");
            SCHEMA_JSON.put("name", "solrAvroFile");
            List<Map<String, Object>> field_info_s = new ArrayList<>();
            Map<String, Object> field_info = new HashMap<>();
            field_info.put("name", "uuid");
            field_info.put("type", "string");
            field_info_s.add(field_info);
            field_info = new HashMap<>();
            field_info.put("name", "file_text");
            field_info.put("type", "string");
            field_info_s.add(field_info);
            field_info = new HashMap<>();
            field_info.put("name", "file_summary");
            field_info.put("type", "string");
            field_info_s.add(field_info);
            SCHEMA_JSON.put("fields", field_info_s);
            SCHEMA = new Schema.Parser().parse(JsonUtil.toJson(SCHEMA_JSON));
            hdfsOperator = new HdfsOperator();
            hdfsOperator.conf.set("fs.hdfs.impl.disable.cache", "false");
        } catch (Exception e) {
            log.error("OCR 提取文字类初始化失败! " + e.getMessage());
        }
    }

    public OcrExtractText(String avroParentDir) {
        this.avroParentDir = avroParentDir;
    }

    public void OCR2AvroAndSolr() {
        log.info("OCR 提取文件的文本摘要并保存到Solr和Avro,开始");
        try {
            List<Path> paths = hdfsOperator.listFiles(avroParentDir, false);
            for (Path path : paths) {
                fixedThreadPool.execute(new OcrTextTask(path));
            }
        } catch (Exception e) {
            throw new BusinessException(String.format("获取目录: [ %s ] 下的avro文件失败! [ %s ]", avroParentDir, e));
        } finally {
            try {
                fixedThreadPool.shutdown();
                boolean b = fixedThreadPool.awaitTermination(Long.MAX_VALUE, TimeUnit.DAYS);
            } catch (InterruptedException e) {
                log.error("关闭线程池失败! 异常" + e);
            }
        }
    }

    public static boolean isOcrFile(String fileName) {
        return OCR_EXPECTED_EXTENSION_S.contains(FileNameUtils.getExtension(fileName));
    }

    @Override
    public void close() {
        if (hdfsOperator != null) {
            hdfsOperator.close();
        }
    }

    class OcrTextTask implements Runnable {

        private final ITesseract instance = new Tesseract();

        private final Path avroPath;

        public OcrTextTask(Path avroPath) {
            this.avroPath = avroPath;
            instance.setDatapath(System.getProperty("user.dir"));
            instance.setLanguage(CommonVariables.OCR_RECOGNITION_LANGUAGE);
        }

        @Override
        public void run() {
            String genAvroPath = avroParentDir + "/ocravro/" + avroPath.getName() + "_zw";
            log.info("正在读取文件: " + avroPath + ", ocr识别到------>" + genAvroPath);
            try {
                if (hdfsOperator.exists(new Path(genAvroPath))) {
                    log.warn("avro文件: " + avroParentDir + "/" + avroPath.getName() + " 已经做过跑批程序! 跳过");
                } else {
                    List<GenericRecord> recordList = readRecordFromAvro(hdfsOperator.fileSystem);
                    if (null != recordList && recordList.size() > 0) {
                        extractTextFromRecord(recordList);
                        putIntoAvro(recordList, genAvroPath, hdfsOperator.fileSystem);
                        putIntoSolr(recordList);
                    }
                }
            } catch (Exception e) {
                log.error("识别文件: " + avroPath + " 到 " + genAvroPath + "失败! 异常:" + e);
            }
        }

        public List<GenericRecord> readRecordFromAvro(FileSystem fs) {
            List<GenericRecord> recordList = new ArrayList<>();
            try (InputStream is = fs.open(avroPath);
                DataFileStream<Object> reader = new DataFileStream<>(is, new GenericDatumReader<>())) {
                log.debug("读取到的文件列表大小: " + reader.getMetaKeys().size());
                while (reader.hasNext()) {
                    GenericRecord r = (GenericRecord) reader.next();
                    recordList.add(r);
                }
                return recordList;
            } catch (IOException e) {
                log.error("Failed to get record from avro file ...", e);
                return null;
            }
        }

        public void extractTextFromRecord(List<GenericRecord> recordList) {
            for (GenericRecord r : recordList) {
                String file_name = r.get("file_name").toString();
                log.debug("待提取的文件名: " + file_name);
                if (!isOcrFile(file_name)) {
                    continue;
                }
                byte[] bytes = ((ByteBuffer) r.get("file_contents")).array();
                String filePath = Bytes.toString(bytes);
                ImageIO.scanForPlugins();
                String result;
                IsFlag is_big_file_flag = IsFlag.ofEnumByCode(r.get("is_big_file").toString());
                if (is_big_file_flag == IsFlag.Shi) {
                    result = extractText(FileUtil.getFile(filePath));
                } else if (is_big_file_flag == IsFlag.Fou) {
                    if (file_name.endsWith(".pdf")) {
                        try {
                            File file = new File(FileUtil.TEMP_DIR_NAME + File.separator + file_name);
                            FileUtils.writeByteArrayToFile(file, bytes);
                            result = extractText(file);
                            boolean wasSuccessful = file.delete();
                            if (!wasSuccessful) {
                                log.warn("文件: " + file.getName() + " ,删除失败!");
                            }
                        } catch (IOException e) {
                            log.error("文件: " + file_name + " ,提取文本失败! 异常: " + e);
                            throw new BusinessException("文件: " + file_name + " ,提取文本失败! 异常: " + e.getMessage());
                        }
                    } else {
                        if (CommonVariables.USE_OCR_RPC) {
                            result = new PictureTextExtract().byteToStr(bytes, FileNameUtils.getExtension(file_name));
                        } else {
                            result = extractText(bytes, file_name);
                        }
                    }
                } else {
                    throw new BusinessException("Neither a large file nor a small file, check the file type!");
                }
                result = result.replaceAll("[\r\n]", " ");
                String summary = TextUtil.etractSummary(result, CommonVariables.SUMMARY_VOLUMN);
                r.put("file_text", result);
                r.put("file_summary", summary);
                safeAdd();
            }
        }

        private void safeAdd() {
            AtomicInteger aci = new AtomicInteger(0);
            int alreadyProccessNumber = aci.incrementAndGet();
            if (alreadyProccessNumber % 100 == 0) {
                log.info("已提取文本：" + alreadyProccessNumber + " 个！！！");
            }
        }

        public String extractText(byte[] fileBytes, String fileName) {
            try (InputStream is = new ByteArrayInputStream(fileBytes)) {
                BufferedImage image = ImageIO.read(is);
                return instance.doOCR(image);
            } catch (Exception e) {
                log.error("提取文件： " + fileName + " 错误！", e);
                return "";
            }
        }

        public String extractText(File picture) {
            try {
                return instance.doOCR(picture);
            } catch (Exception e) {
                log.error("提取文件： " + picture.getName() + " 错误！", e);
                return "";
            }
        }

        public void putIntoAvro(List<GenericRecord> recordList, String genAvroPath, FileSystem fs) {
            try (OutputStream outputStream = fs.create(new Path(genAvroPath));
                DataFileWriter<Object> writer = new DataFileWriter<>(new GenericDatumWriter<>()).setSyncInterval(100)) {
                writer.setCodec(CodecFactory.snappyCodec());
                writer.create(SCHEMA, outputStream);
                for (GenericRecord genericRecord : recordList) {
                    GenericRecord newGenericRecord = new GenericData.Record(SCHEMA);
                    newGenericRecord.put("uuid", genericRecord.get("uuid").toString());
                    newGenericRecord.put("file_text", genericRecord.get("file_text").toString());
                    newGenericRecord.put("file_summary", genericRecord.get("file_summary").toString());
                    writer.append(newGenericRecord);
                }
            } catch (Exception e) {
                log.error("Failed to write to avro file ...", e);
            }
        }

        public void putIntoSolr(List<GenericRecord> recordList) {
            try (ISolrOperator os = SolrFactory.getSolrOperatorInstance()) {
                SolrClient server = os.getSolrClient();
                List<SolrInputDocument> docs = new ArrayList<>();
                SolrInputDocument doc;
                for (GenericRecord r : recordList) {
                    String id = r.get("uuid").toString();
                    log.debug("Put到solr, file_id: " + id);
                    String text = r.get("file_text").toString().replace("\n", "\\n");
                    if (StringUtil.isBlank(text)) {
                        continue;
                    }
                    doc = new SolrInputDocument();
                    doc.addField("id", r.get("uuid").toString());
                    doc.addField("tf-file_name", r.get("file_name").toString());
                    doc.addField("tf-file_scr_path", r.get("file_scr_path").toString());
                    doc.addField("tf-file_size", r.get("file_size").toString());
                    doc.addField("tf-file_time", r.get("file_time").toString());
                    doc.addField("tf-file_summary", r.get("file_summary").toString());
                    doc.addField("tf-file_text", text);
                    doc.addField("tf-file_md5", r.get("file_md5").toString());
                    doc.addField("tf-file_avro_path", r.get("file_avro_path").toString());
                    doc.addField("tf-file_avro_block", r.get("file_avro_block").toString());
                    doc.addField("tf-is_big_file", r.get("is_big_file").toString());
                    docs.add(doc);
                }
                if (docs.size() != 0) {
                    server.add(docs);
                    server.commit();
                }
            } catch (Exception e) {
                log.error("Failed to write to solr ...", e);
            }
        }
    }

    public static void main(String[] args) {
        log.info("Start~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
        OcrExtractText oet = new OcrExtractText("/hrds/DCL/829016311534194688/833707013941760000");
        oet.OCR2AvroAndSolr();
        log.info("End~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~");
    }
}
