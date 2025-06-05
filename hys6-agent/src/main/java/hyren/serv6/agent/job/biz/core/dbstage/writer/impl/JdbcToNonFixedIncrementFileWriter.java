package hyren.serv6.agent.job.biz.core.dbstage.writer.impl;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.agent.job.biz.utils.WriterFile;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DataExtractionDef;
import hyren.serv6.base.exception.AppSystemException;
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
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcToNonFixedIncrementFileWriter extends AbstractFileWriter {

    private final boolean writeHeaderFlag;

    public JdbcToNonFixedIncrementFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef, boolean writeHeaderFlag) {
        super(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
        this.writeHeaderFlag = writeHeaderFlag;
    }

    @Override
    public String writeFiles() {
        DataFileWriter<Object> avroWriter = null;
        BufferedWriter writer;
        long counter = 0;
        int index = 0;
        WriterFile writerFile = null;
        StringBuilder fileInfo = new StringBuilder(1024);
        String hbase_name = collectTableBean.getStorage_table_name();
        String eltDate = collectTableBean.getEtlDate();
        String plane_url = dataExtractionDef.getPlane_url();
        String midName = plane_url + File.separator + eltDate + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(FileFormat.FeiDingChang.getCode()) + File.separator;
        try {
            String database_code = dataExtractionDef.getDatabase_code();
            midName = FileNameUtils.normalize(midName, true);
            String fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
            fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
            writerFile = new WriterFile(fileName);
            writer = writerFile.getIncrementBufferedWriter(DataBaseCode.ofValueByCode(database_code));
            writeHeader(writer, tableBean.getColumnMetaInfo());
            List<String> queryColumnList = new ArrayList<>();
            Map<String, Integer> typeValueMap = new HashMap<>();
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                queryColumnList.add(rsMetaData.getColumnName(i).toUpperCase());
                typeValueMap.put(rsMetaData.getColumnName(i).toUpperCase(), rsMetaData.getColumnType(i));
            }
            avroWriter = getAvroWriter(typeValueMap, hbase_name, midName, pageNum);
            List<String> allColumnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
            StringBuilder sb_ = new StringBuilder();
            StringBuilder line = new StringBuilder();
            String operate = tableBean.getOperate();
            while (resultSet.next()) {
                line.append(operate).append(dataExtractionDef.getDatabase_separatorr());
                counter++;
                for (String column_name : allColumnList) {
                    if (queryColumnList.contains(column_name)) {
                        getOneColumnValue(avroWriter, counter, pageNum, resultSet, typeValueMap.get(column_name), sb_, column_name, hbase_name, midName);
                        line.append(sb_.toString());
                        sb_.delete(0, sb_.length());
                    }
                    line.append(dataExtractionDef.getDatabase_separatorr());
                }
                line.delete(line.length() - dataExtractionDef.getDatabase_separatorr().length(), line.length());
                line.append(dataExtractionDef.getRow_separator());
                writer.write(line.toString());
                if (counter % JobConstant.BUFFER_ROW == 0) {
                    log.info("表 : " + hbase_name + ", 正在写入文件，已写入" + counter + "行");
                    writer.flush();
                }
                line.delete(0, line.length());
            }
            writer.flush();
        } catch (Exception e) {
            log.error("表" + collectTableBean.getTable_name() + "数据库增量抽取卸数文件失败", e);
            throw new AppSystemException("数据库增量抽取卸数文件失败", e);
        } finally {
            try {
                if (writerFile != null)
                    writerFile.incrementBufferedWriterClose();
                if (avroWriter != null)
                    avroWriter.close();
            } catch (IOException e) {
                log.error("关闭流失败", e);
            }
        }
        fileInfo.append(counter);
        return fileInfo.toString();
    }

    private void writeHeader(BufferedWriter writer, String columnMetaInfo) throws Exception {
        if (IsFlag.Shi.getCode().equals(dataExtractionDef.getIs_header()) && writeHeaderFlag) {
            if (!StringUtil.isEmpty(dataExtractionDef.getDatabase_separatorr())) {
                columnMetaInfo = StringUtil.replace(columnMetaInfo, Constant.METAINFOSPLIT, dataExtractionDef.getDatabase_separatorr());
                columnMetaInfo = "operate," + columnMetaInfo;
            } else {
                throw new AppSystemException("非定长文件，列分隔符不能为空");
            }
            writer.write(columnMetaInfo);
            writer.write(dataExtractionDef.getRow_separator());
        }
    }
}
