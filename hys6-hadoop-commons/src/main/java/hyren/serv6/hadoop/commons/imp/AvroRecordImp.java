package hyren.serv6.hadoop.commons.imp;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.utils.CodecUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.i.IAvroRecord;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.hadoop.commons.ocr.OcrExtractText;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.PathUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.avro.mapred.FsInput;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.Path;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class AvroRecordImp implements IAvroRecord {

    @Override
    public byte[] getFileBytesFromAvro(Long fileAvroBlock, String fileAvroPath) {
        GenericRecord avroRecord = getAvroRecord(fileAvroBlock, fileAvroPath);
        String isBigFile = avroRecord.get("is_big_file").toString();
        if (IsFlag.Shi.getCode().equals(isBigFile)) {
            String filePath = new String(((ByteBuffer) avroRecord.get("file_contents")).array(), CodecUtil.UTF8_CHARSET);
            String fileHdfsPath = PathUtil.convertLocalPathToHDFSPath(filePath);
            try (HdfsOperator operator = new HdfsOperator()) {
                if (!operator.exists(fileHdfsPath)) {
                    throw new BusinessException("大文件 " + fileHdfsPath + " 不存在");
                }
                return IOUtils.toByteArray(operator.open(fileHdfsPath));
            } catch (IOException e) {
                e.printStackTrace();
                throw new BusinessException("Failed to get the byte of the hdfs file!" + fileAvroPath);
            }
        } else {
            return ((ByteBuffer) avroRecord.get("file_contents")).array();
        }
    }

    public Map<String, Object> getFileContents(Long fileAvroBlock, String fileAvroPath) {
        Map<String, Object> map = new HashMap<>();
        GenericRecord avroRecord = getAvroRecord(fileAvroBlock, fileAvroPath);
        map.put("fileText", avroRecord.get("file_text").toString());
        map.put("file_contents", (ByteBuffer) avroRecord.get("file_contents"));
        return map;
    }

    public String getOcrText(String file_avro_path, String name, String uuid) {
        String file_text = "";
        if (OcrExtractText.isOcrFile(name)) {
            Path path = new Path(file_avro_path);
            String ocrpath = path.getParent() + "/ocravro/" + path.getName() + "_zw";
            try {
                SeekableInput ocrin;
                HdfsOperator hdfsOperator = new HdfsOperator();
                if (hdfsOperator.exists(ocrpath)) {
                    ocrin = new FsInput(new Path(ocrpath), hdfsOperator.conf);
                    DatumReader<GenericRecord> ocrreader = new GenericDatumReader<>();
                    DataFileReader<GenericRecord> ocrfileReader = new DataFileReader<>(ocrin, ocrreader);
                    GenericRecord ocrrecord = new GenericData.Record(ocrfileReader.getSchema());
                    while (ocrfileReader.hasNext()) {
                        GenericRecord ocrgrnext = ocrfileReader.next(ocrrecord);
                        if (ocrgrnext.get("uuid").toString().equals(uuid)) {
                            file_text = ocrgrnext.get("file_text").toString();
                            break;
                        }
                    }
                }
            } catch (Exception e) {
                log.error("Get ocr text error:", e);
            }
        }
        return file_text;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_avro_block", desc = "", range = "")
    @Param(name = "file_avro_path", desc = "", range = "")
    private static GenericRecord getAvroRecord(Long file_avro_block, String file_avro_path) {
        Path path = new Path(file_avro_path);
        DatumReader<GenericRecord> reader = new GenericDatumReader<>();
        if (CommonVariables.FILE_COLLECTION_IS_WRITE_HADOOP) {
            try (SeekableInput in = new FsInput(path, new HdfsOperator().conf);
                DataFileReader<GenericRecord> fileReader = new DataFileReader<>(in, reader)) {
                GenericRecord record = new GenericData.Record(fileReader.getSchema());
                fileReader.seek(file_avro_block);
                if (fileReader.hasNext()) {
                    return fileReader.next(record);
                } else {
                    throw new BusinessException("This block has no record in avro");
                }
            } catch (Exception e) {
                throw new BusinessException("Failed to get hdfs file! " + e.getMessage());
            }
        } else {
            File avro_file = new File(path.toString());
            if (!avro_file.exists()) {
                throw new BusinessException("本地文件: " + path + " ,已经不存在!");
            }
            try (DataFileReader<GenericRecord> fileReader = new DataFileReader<>(avro_file, reader)) {
                log.info("读取本地文件: " + path);
                GenericRecord record = new GenericData.Record(fileReader.getSchema());
                fileReader.seek(file_avro_block);
                if (fileReader.hasNext()) {
                    return fileReader.next(record);
                } else {
                    throw new BusinessException("This block has no record in avro!");
                }
            } catch (IOException e) {
                throw new BusinessException("Failed to get local file! " + e.getMessage());
            }
        }
    }
}
