package hyren.serv6.hadoop.commons.imp.writer.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.CharSplitType;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.commons.dataclean.Clean;
import hyren.serv6.commons.dataclean.CleanFactory;
import hyren.serv6.commons.dataclean.DataCleanInterface;
import hyren.serv6.base.entity.ColumnSplit;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.writer.AbstractFileWriter;
import hyren.serv6.hadoop.utils.ColUtil;
import hyren.serv6.hadoop.utils.ColumnTool;
import hyren.serv6.hadoop.utils.ParquetUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.file.DataFileWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.schema.MessageType;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2023-06-07 17:32:28")
@Slf4j
public class JdbcToParquetFileWriter extends AbstractFileWriter {

    public JdbcToParquetFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef) {
        super(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
    }

    @SuppressWarnings("unchecked")
    @Override
    public String writeFiles() {
        String eltDate = collectTableBean.getEtlDate();
        StringBuilder fileInfo = new StringBuilder(1024);
        String hbase_name = collectTableBean.getStorage_table_name();
        String plane_url = dataExtractionDef.getPlane_url();
        String midName = plane_url + File.separator + eltDate + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(FileFormat.PARQUET.getCode()) + File.separator;
        midName = FileNameUtils.normalize(midName, true);
        String dataDelimiter = dataExtractionDef.getDatabase_separatorr();
        DataFileWriter<Object> avroWriter = null;
        ParquetWriter<Group> parquetWriter = null;
        long counter = 0;
        int index = 0;
        GroupFactory factory;
        try {
            avroWriter = getAvroWriter(tableBean.getTypeArray(), hbase_name, midName, pageNum);
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
            String currValue;
            int numberOfColumns = selectColumnList.size();
            int[] typeArray = tableBean.getTypeArray();
            MessageType parquetSchema = ParquetUtil.getSchema(tableBean.getColumnMetaInfo(), tableBean.getColTypeMetaInfo());
            factory = new SimpleGroupFactory(parquetSchema);
            String fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
            parquetWriter = ParquetUtil.getParquetWriter(parquetSchema, fileName);
            fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
            List<String> type = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
            while (resultSet.next()) {
                counter++;
                md5StringTmp.delete(0, md5StringTmp.length());
                mergeStringTmp.delete(0, mergeStringTmp.length());
                Group group = factory.newGroup();
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
                    Map<String, Map<Integer, String>> ordering = (Map<String, Map<Integer, String>>) parseJson.get("ordering");
                    String columnName = selectColumnList.get(i).toUpperCase();
                    if (!ordering.isEmpty()) {
                        Map<Integer, String> colMap = ordering.get(columnName);
                        for (int j = 1; j <= colMap.size(); j++) {
                            if (colMap.get(i).equals("6")) {
                                currValue = split(splitIng, currValue, columnName, type.get(i), group, dataDelimiter);
                            } else {
                                currValue = cl.cleanColumn(currValue, columnName, type.get(i), FileFormat.PARQUET.getCode(), null, dataExtractionDef.getDatabase_code(), dataDelimiter);
                            }
                        }
                    }
                    if (splitIng.get(columnName) == null || splitIng.get(columnName).size() == 0) {
                        ColumnTool.addData2Group(group, type.get(i), selectColumnList.get(i), currValue);
                    }
                }
                if (!mergeIng.isEmpty()) {
                    List<String> arrColString = StringUtil.split(mergeStringTmp.toString(), Constant.DATADELIMITER);
                    merge(mergeIng, arrColString.toArray(new String[0]), selectColumnList.toArray(new String[0]), group);
                }
                if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_md5())) {
                    String md5 = toMD5(md5StringTmp.toString());
                    group.append(Constant._HYREN_E_DATE, Constant._MAX_DATE_8).append(Constant._HYREN_MD5_VAL, md5);
                }
                appendOperateInfo(group);
                if (JobConstant.WriteMultipleFiles) {
                    long messageSize = group.toString().length();
                    long singleFileSize = new File(fileName).length();
                    if (singleFileSize + messageSize > JobConstant.FILE_BLOCKSIZE) {
                        parquetWriter.close();
                        index++;
                        fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
                        parquetWriter = ParquetUtil.getParquetWriter(parquetSchema, fileName);
                        fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
                    }
                }
                parquetWriter.write(group);
                if (counter % JobConstant.BUFFER_ROW == 0) {
                    log.info("表 : " + hbase_name + ", 正在写入文件，已写入" + counter + "行");
                }
            }
        } catch (Exception e) {
            log.error("卸数失败", e);
            throw new AppSystemException("数据库采集卸数Parquet文件失败" + e.getMessage());
        } finally {
            try {
                if (parquetWriter != null)
                    parquetWriter.close();
                if (avroWriter != null)
                    avroWriter.close();
            } catch (IOException e) {
                log.error(e.getMessage(), e);
            }
        }
        fileInfo.append(counter);
        return fileInfo.toString();
    }

    public void merge(Map<String, String> mergeing, String[] arrColString, String[] columns, Group group) {
        if (mergeing.size() != 0) {
            ColUtil colUtil = new ColUtil();
            for (String key : mergeing.keySet()) {
                StringBuilder sb = new StringBuilder();
                int[] index = findColIndex(columns, mergeing.get(key));
                for (int i : index) {
                    sb.append(arrColString[i]);
                }
                List<String> split = StringUtil.split(key, Constant.METAINFOSPLIT);
                colUtil.addData2Group(group, split.get(1).toUpperCase(), split.get(0).toUpperCase(), sb.toString());
            }
        }
    }

    private int[] findColIndex(String[] column, String str) {
        List<String> split = StringUtil.split(str, ",");
        int[] index = new int[split.size()];
        for (int i = 0; i < split.size(); i++) {
            for (int j = 0; j < column.length; j++) {
                if (split.get(i).equalsIgnoreCase(column[j])) {
                    index[i] = j;
                }
            }
        }
        return index;
    }

    private String split(Map<String, Map<String, ColumnSplit>> spliting, String columnData, String columnName, String type, Group group, String database_separatorr) {
        if (spliting.get(columnName) != null && spliting.get(columnName).size() > 0) {
            StringBuilder sb = new StringBuilder(4096);
            ColUtil colUtil = new ColUtil();
            if (group != null) {
                colUtil.addData2Group(group, type, columnName, columnData);
            }
            sb.append(columnData);
            Map<String, ColumnSplit> colMap = spliting.get(columnName);
            ColumnSplit cp;
            if (null != colMap && colMap.size() != 0) {
                for (String colName : colMap.keySet()) {
                    cp = colMap.get(colName);
                    if (StringUtil.isEmpty(columnData)) {
                        if (group != null) {
                            colUtil.addData2Group(group, cp.getCol_type(), colName.toUpperCase(), "");
                        }
                    } else {
                        try {
                            String substr;
                            if (CharSplitType.PianYiLiang.getCode().equals(cp.getSplit_type())) {
                                String col_offset = cp.getCol_offset();
                                String[] split = col_offset.split(",");
                                int start = Integer.parseInt(split[0]);
                                int end = Integer.parseInt(split[1]);
                                substr = columnData.substring(start, end);
                            } else if (CharSplitType.ZhiDingFuHao.getCode().equals(cp.getSplit_type())) {
                                int num = cp.getSeq() == null ? 0 : Integer.parseInt(cp.getSeq().toString());
                                List<String> splitInNull = StringUtil.split(columnData, cp.getSplit_sep());
                                substr = splitInNull.get(num);
                            } else {
                                throw new AppSystemException("不支持的字符拆分方式");
                            }
                            if (group != null) {
                                colUtil.addData2Group(group, cp.getCol_type(), colName.toUpperCase(), substr);
                            }
                        } catch (Exception e) {
                            throw new AppSystemException("请检查" + colName + "字段定义的字符拆分的方式" + e.getMessage());
                        }
                    }
                }
            }
            if (database_separatorr.length() > 0 && sb.length() > database_separatorr.length()) {
                sb.delete(sb.length() - database_separatorr.length(), sb.length());
            }
            return sb.toString();
        } else {
            return columnData;
        }
    }

    private void appendOperateInfo(Group group) {
        if (JobConstant.ISADDOPERATEINFO) {
            group.append(Constant._HYREN_OPER_DATE, operateDate);
            group.append(Constant._HYREN_OPER_TIME, operateTime);
            group.append(Constant._HYREN_OPER_PERSON, user_id);
        }
    }
}
