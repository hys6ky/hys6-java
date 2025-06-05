package hyren.serv6.hadoop.commons.imp.writer.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.dataclean.Clean;
import hyren.serv6.commons.dataclean.CleanFactory;
import hyren.serv6.commons.dataclean.DataCleanInterface;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.writer.AbstractFileWriter;
import hyren.serv6.hadoop.utils.ColumnTool;
import hyren.serv6.hadoop.utils.WriterFile;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.hadoop.hive.ql.io.orc.OrcSerde;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.RecordWriter;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2023-06-07 17:32:11")
@Slf4j
public class JdbcToOrcFileWriter extends AbstractFileWriter {

    private final OrcSerde serde = new OrcSerde();

    public JdbcToOrcFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        super(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String writeFiles() {
        String eltDate = collectTableBean.getEtlDate();
        String plane_url = dataExtractionDef.getPlane_url();
        String midName = plane_url + File.separator + eltDate + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(FileFormat.ORC.getCode()) + File.separator;
        String hbase_name = collectTableBean.getStorage_table_name();
        midName = FileNameUtils.normalize(midName, true);
        String dataDelimiter = dataExtractionDef.getDatabase_separatorr();
        DataFileWriter<Object> avroWriter = null;
        long counter = 0;
        int index = 0;
        WriterFile writerFile = null;
        RecordWriter<NullWritable, Writable> writer;
        StringBuilder fileInfo = new StringBuilder(1024);
        try {
            String fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
            fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
            writerFile = new WriterFile(fileName);
            avroWriter = getAvroWriter(tableBean.getTypeArray(), hbase_name, midName, pageNum);
            writer = writerFile.getOrcWrite();
            final DataCleanInterface allClean = CleanFactory.getInstance().getObjectClean("clean_database");
            List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
            Map<String, Object> parseJson = tableBean.getParseJson();
            Map<String, String> mergeIng = (Map<String, String>) parseJson.get("mergeIng");
            Map<String, Map<String, ColumnSplit>> splitIng = (Map<String, Map<String, ColumnSplit>>) parseJson.get("splitIng");
            Clean cl = new Clean(parseJson, allClean);
            StringBuilder mergeStringTmp = new StringBuilder(1024 * 1024);
            Map<String, Boolean> md5Col = transMd5ColMap(tableBean.getIsZipperFieldInfo());
            StringBuilder md5StringTmp = new StringBuilder(1024 * 1024);
            StringBuilder sb_ = new StringBuilder();
            List<String> typeList = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
            String currValue;
            int numberOfColumns = selectColumnList.size();
            int[] typeArray = tableBean.getTypeArray();
            StructObjectInspector inspector = ColumnTool.schemaInfo(tableBean.getColumnMetaInfo(), tableBean.getColTypeMetaInfo());
            List<Object> lineData;
            while (resultSet.next()) {
                counter++;
                md5StringTmp.delete(0, md5StringTmp.length());
                mergeStringTmp.delete(0, mergeStringTmp.length());
                lineData = new ArrayList<>();
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
                    currValue = cl.cleanColumn(currValue, selectColumnList.get(i).toUpperCase(), typeList.get(i), FileFormat.ORC.getCode(), lineData, dataExtractionDef.getDatabase_code(), dataDelimiter);
                    if (splitIng.get(selectColumnList.get(i).toUpperCase()) == null || splitIng.get(selectColumnList.get(i).toUpperCase()).isEmpty()) {
                        ColumnTool.addData2Inspector(lineData, typeList.get(i), currValue);
                    }
                }
                if (!mergeIng.isEmpty()) {
                    List<String> arrColString = StringUtil.split(mergeStringTmp.toString(), Constant.DATADELIMITER);
                    allClean.merge(mergeIng, arrColString.toArray(new String[0]), selectColumnList.toArray(new String[0]), lineData, FileFormat.ORC.getCode(), dataExtractionDef.getDatabase_code(), dataDelimiter);
                }
                if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_md5())) {
                    String md5 = toMD5(md5StringTmp.toString());
                    lineData.add(Constant._MAX_DATE_8);
                    lineData.add(md5);
                }
                appendOperateInfo(lineData);
                if (JobConstant.WriteMultipleFiles) {
                    long messageSize = lineData.toString().length();
                    long singleFileSize = new File(fileName).length();
                    if (singleFileSize + messageSize > JobConstant.FILE_BLOCKSIZE) {
                        writerFile.close();
                        index++;
                        fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
                        writerFile = new WriterFile(fileName);
                        writer = writerFile.getOrcWrite();
                        fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
                    }
                }
                writer.write(NullWritable.get(), serde.serialize(lineData, inspector));
                if (counter % JobConstant.BUFFER_ROW == 0) {
                    log.info("表 : " + hbase_name + ", 正在写入文件，已写入" + counter + "行");
                }
            }
        } catch (Exception e) {
            log.error("卸数失败", e);
            throw new AppSystemException("数据库采集卸数Orc文件失败" + e.getMessage());
        } finally {
            try {
                if (writerFile != null)
                    writerFile.orcClose();
                if (avroWriter != null)
                    avroWriter.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        fileInfo.append(counter);
        return fileInfo.toString();
    }

    private void appendOperateInfo(List<Object> sb) {
        if (JobConstant.ISADDOPERATEINFO) {
            sb.add(operateDate);
            sb.add(operateTime);
            sb.add(user_id);
        }
    }
}
