package hyren.serv6.agent.job.biz.core.dbstage.writer.impl;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.agent.job.biz.utils.WriterFile;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.dataclean.Clean;
import hyren.serv6.commons.dataclean.CleanFactory;
import hyren.serv6.commons.dataclean.DataCleanInterface;
import hyren.serv6.commons.hadoop.writer.AbstractFileWriter;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcToFixedFileWriter extends AbstractFileWriter {

    public JdbcToFixedFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        super(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String writeFiles() {
        String database_separatorr = dataExtractionDef.getDatabase_separatorr() == null ? "" : dataExtractionDef.getDatabase_separatorr();
        String eltDate = collectTableBean.getEtlDate();
        StringBuilder fileInfo = new StringBuilder(1024);
        String hbase_name = collectTableBean.getStorage_table_name();
        String plane_url = dataExtractionDef.getPlane_url();
        String midName = plane_url + File.separator + eltDate + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(FileFormat.DingChang.getCode()) + File.separator;
        midName = FileNameUtils.normalize(midName, true);
        DataFileWriter<Object> avroWriter = null;
        BufferedWriter writer;
        long counter = 0;
        int index = 0;
        WriterFile writerFile = null;
        try {
            String database_code = dataExtractionDef.getDatabase_code();
            avroWriter = getAvroWriter(tableBean.getTypeArray(), hbase_name, midName, pageNum);
            String fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
            fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
            writerFile = new WriterFile(fileName);
            writer = writerFile.getBufferedWriter(DataBaseCode.ofValueByCode(database_code));
            writeHeader(writer, tableBean.getColumnMetaInfo());
            final DataCleanInterface allclean = CleanFactory.getInstance().getObjectClean("clean_database");
            List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
            Map<String, Object> parseJson = tableBean.getParseJson();
            Map<String, String> mergeIng = (Map<String, String>) parseJson.get("mergeIng");
            Map<String, Map<String, ColumnSplit>> splitIng = (Map<String, Map<String, ColumnSplit>>) parseJson.get("splitIng");
            Clean cl = new Clean(parseJson, allclean);
            StringBuilder mergeStringTmp = new StringBuilder(1024 * 1024);
            Map<String, Boolean> md5Col = transMd5ColMap(tableBean.getIsZipperFieldInfo());
            StringBuilder md5StringTmp = new StringBuilder(1024 * 1024);
            StringBuilder sb = new StringBuilder();
            StringBuilder sb_ = new StringBuilder();
            List<String> typeList = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
            int numberOfColumns = selectColumnList.size();
            log.info("type : " + typeList.size() + "  colName " + numberOfColumns);
            String currValue;
            int[] typeArray = tableBean.getTypeArray();
            while (resultSet.next()) {
                counter++;
                md5StringTmp.delete(0, md5StringTmp.length());
                mergeStringTmp.delete(0, mergeStringTmp.length());
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
                    currValue = cl.cleanColumn(currValue, selectColumnList.get(i).toUpperCase(), typeList.get(i), FileFormat.DingChang.getCode(), null, database_code, database_separatorr);
                    if (splitIng.get(selectColumnList.get(i).toUpperCase()) == null || splitIng.get(selectColumnList.get(i).toUpperCase()).isEmpty()) {
                        String type = typeList.get(i);
                        sb.append(columnToFixed(currValue, TypeTransLength.getLength(type, tableBean.getDatabase_type()), database_code, selectColumnList.get(i))).append(database_separatorr);
                    } else {
                        sb.append(currValue).append(database_separatorr);
                    }
                }
                if (!mergeIng.isEmpty()) {
                    List<String> arrColString = StringUtil.split(mergeStringTmp.toString(), Constant.DATADELIMITER);
                    String merge = allclean.merge(mergeIng, arrColString.toArray(new String[0]), selectColumnList.toArray(new String[0]), null, FileFormat.DingChang.getCode(), database_code, database_separatorr);
                    sb.append(merge).append(database_separatorr);
                }
                if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_md5())) {
                    String md5 = toMD5(md5StringTmp.toString());
                    sb.append(database_separatorr).append(Constant._MAX_DATE_8).append(database_separatorr).append(md5);
                }
                appendOperateInfo(sb, database_separatorr);
                sb.append(dataExtractionDef.getRow_separator());
                if (JobConstant.WriteMultipleFiles) {
                    long messageSize = sb.toString().length();
                    long singleFileSize = new File(fileName).length();
                    if (singleFileSize + messageSize > JobConstant.FILE_BLOCKSIZE) {
                        writerFile.bufferedWriterClose();
                        index++;
                        fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
                        writerFile = new WriterFile(fileName);
                        writer = writerFile.getBufferedWriter(DataBaseCode.ofValueByCode(database_code));
                        writeHeader(writer, tableBean.getColumnMetaInfo());
                        fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
                    }
                }
                writer.write(sb.toString());
                if (counter % JobConstant.BUFFER_ROW == 0) {
                    log.info("表 : " + hbase_name + ", 正在写入文件，已写入" + counter + "行");
                    writer.flush();
                }
                sb.delete(0, sb.length());
            }
            writer.flush();
        } catch (Exception e) {
            log.error("卸数失败", e);
            throw new AppSystemException("数据库采集卸数定长文件失败", e);
        } finally {
            try {
                if (writerFile != null)
                    writerFile.bufferedWriterClose();
                if (avroWriter != null)
                    avroWriter.close();
            } catch (IOException e) {
                log.error("关闭流失败：", e);
            }
        }
        fileInfo.append(counter);
        return fileInfo.toString();
    }

    private void writeHeader(BufferedWriter writer, String columnMetaInfo) throws Exception {
        if (IsFlag.Shi.getCode().equals(dataExtractionDef.getIs_header())) {
            if (!StringUtil.isEmpty(dataExtractionDef.getDatabase_separatorr())) {
                columnMetaInfo = StringUtil.replace(columnMetaInfo, Constant.METAINFOSPLIT, dataExtractionDef.getDatabase_separatorr());
            } else {
                columnMetaInfo = StringUtil.replace(columnMetaInfo, Constant.METAINFOSPLIT, ",");
            }
            writer.write(columnMetaInfo);
            writer.write(dataExtractionDef.getRow_separator());
        }
    }
}
