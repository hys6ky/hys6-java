package hyren.serv6.hadoop.commons.imp.writer.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.dataclean.Clean;
import hyren.serv6.commons.dataclean.CleanFactory;
import hyren.serv6.commons.dataclean.DataCleanInterface;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.writer.AbstractFileWriter;
import hyren.serv6.hadoop.utils.WriterFile;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2023-06-07 15:50:04")
@Slf4j
public class JdbcToSequenceFileWriter extends AbstractFileWriter {

    public JdbcToSequenceFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        super(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String writeFiles() {
        String etlDate = collectTableBean.getEtlDate();
        StringBuilder fileInfo = new StringBuilder(1024);
        String hbase_name = collectTableBean.getStorage_table_name();
        String table_name = collectTableBean.getTable_name();
        String midName = dataExtractionDef.getPlane_url() + File.separator + etlDate + File.separator + table_name + File.separator + Constant.fileFormatMap.get(FileFormat.SEQUENCEFILE.getCode()) + File.separator;
        String dataDelimiter = Constant.SEQUENCEDELIMITER;
        midName = FileNameUtils.normalize(midName, true);
        DataFileWriter<Object> avroWriter = null;
        long counter = 0;
        int index = 0;
        WriterFile writerFile = null;
        SequenceFile.Writer writer;
        Text value;
        try {
            avroWriter = getAvroWriter(tableBean.getTypeArray(), hbase_name, midName, pageNum);
            String fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
            fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
            writerFile = new WriterFile(fileName);
            writer = writerFile.getSequenceWrite();
            final DataCleanInterface allclean = CleanFactory.getInstance().getObjectClean("clean_database");
            List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
            Map<String, Object> parseJson = tableBean.getParseJson();
            Map<String, String> mergeIng = (Map<String, String>) parseJson.get("mergeIng");
            Clean cl = new Clean(parseJson, allclean);
            StringBuilder mergeStringTmp = new StringBuilder(1024 * 1024);
            Map<String, Boolean> md5Col = transMd5ColMap(tableBean.getIsZipperFieldInfo());
            StringBuilder md5StringTmp = new StringBuilder(1024 * 1024);
            StringBuilder sb = new StringBuilder();
            StringBuilder sb_ = new StringBuilder();
            String currValue;
            int numberOfColumns = selectColumnList.size();
            List<String> type = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
            int[] typeArray = tableBean.getTypeArray();
            while (resultSet.next()) {
                counter++;
                md5StringTmp.delete(0, md5StringTmp.length());
                mergeStringTmp.delete(0, mergeStringTmp.length());
                value = new Text();
                for (int i = 0; i < numberOfColumns; i++) {
                    sb_.delete(0, sb_.length());
                    mergeStringTmp.append(getOneColumnValue(avroWriter, counter, pageNum, resultSet, typeArray[i], sb_, selectColumnList.get(i), hbase_name, midName));
                    if (i < numberOfColumns - 1) {
                        mergeStringTmp.append(Constant.DATADELIMITER);
                    }
                    currValue = sb_.toString();
                    if (md5Col.get(selectColumnList.get(i)) != null && md5Col.get(selectColumnList.get(i))) {
                        md5StringTmp.append(currValue);
                    }
                    currValue = cl.cleanColumn(currValue, selectColumnList.get(i).toUpperCase(), type.get(i), FileFormat.SEQUENCEFILE.getCode(), null, dataExtractionDef.getDatabase_code(), dataDelimiter);
                    sb.append(currValue).append(dataDelimiter);
                }
                if (!mergeIng.isEmpty()) {
                    List<String> arrColString = StringUtil.split(mergeStringTmp.toString(), Constant.DATADELIMITER);
                    String mer = allclean.merge(mergeIng, arrColString.toArray(new String[0]), selectColumnList.toArray(new String[0]), null, FileFormat.SEQUENCEFILE.getCode(), dataExtractionDef.getDatabase_code(), dataDelimiter);
                    sb.append(mer).append(dataDelimiter);
                }
                if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_md5())) {
                    String md5 = toMD5(md5StringTmp.toString());
                    sb.append(dataDelimiter).append(Constant._MAX_DATE_8).append(dataDelimiter).append(md5);
                }
                appendOperateInfo(sb, dataDelimiter);
                if (JobConstant.WriteMultipleFiles) {
                    long messageSize = sb.toString().length();
                    long singleFileSize = new File(midName + index + ".part").length();
                    if (singleFileSize + messageSize > JobConstant.FILE_BLOCKSIZE) {
                        writerFile.sequenceClose();
                        index++;
                        fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
                        writerFile = new WriterFile(fileName);
                        writer = writerFile.getSequenceWrite();
                        fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
                    }
                }
                value.set(sb.toString());
                writer.append(NullWritable.get(), value);
                if (counter % JobConstant.BUFFER_ROW == 0) {
                    log.info(hbase_name + "文件已写入一次,目前写到" + counter + "行");
                    writer.hflush();
                }
                sb.delete(0, sb.length());
            }
            writer.hflush();
        } catch (Exception e) {
            log.error("卸数失败", e);
            throw new AppSystemException("数据库采集卸数Sequence文件失败" + e.getMessage());
        } finally {
            try {
                if (writerFile != null)
                    writerFile.sequenceClose();
                if (avroWriter != null)
                    avroWriter.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        fileInfo.append(counter);
        return fileInfo.toString();
    }
}
