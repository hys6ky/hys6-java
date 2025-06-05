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
import org.supercsv.io.CsvListWriter;
import java.io.File;
import java.io.IOException;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class JdbcToCsvIncrementFileWriter extends AbstractFileWriter {

    private final boolean writeHeaderFlag;

    public JdbcToCsvIncrementFileWriter(ResultSet resultSet, CollectTableBean collectTableBean, int pageNum, TableBean tableBean, DataExtractionDef dataExtractionDef, boolean writeHeaderFlag) {
        super(resultSet, collectTableBean, pageNum, tableBean, dataExtractionDef);
        this.writeHeaderFlag = writeHeaderFlag;
    }

    @Override
    public String writeFiles() {
        DataFileWriter<Object> avroWriter = null;
        CsvListWriter writer;
        long counter = 0;
        int index = 0;
        WriterFile writerFile = null;
        StringBuilder fileInfo = new StringBuilder(1024);
        String hbase_name = collectTableBean.getStorage_table_name();
        String eltDate = collectTableBean.getEtlDate();
        String plane_url = dataExtractionDef.getPlane_url();
        String midName = plane_url + File.separator + eltDate + File.separator + collectTableBean.getTable_name() + File.separator + Constant.fileFormatMap.get(FileFormat.CSV.getCode()) + File.separator;
        try {
            String database_code = dataExtractionDef.getDatabase_code();
            midName = FileNameUtils.normalize(midName, true);
            String fileName = midName + hbase_name + pageNum + index + "." + dataExtractionDef.getFile_suffix();
            fileInfo.append(fileName).append(Constant.METAINFOSPLIT);
            writerFile = new WriterFile(fileName);
            writer = writerFile.getIncrementCsvWriter(DataBaseCode.ofValueByCode(database_code));
            List<String> queryColumnList = new ArrayList<>();
            Map<String, Integer> typeValueMap = new HashMap<>();
            ResultSetMetaData rsMetaData = resultSet.getMetaData();
            for (int i = 1; i <= rsMetaData.getColumnCount(); i++) {
                queryColumnList.add(rsMetaData.getColumnName(i).toUpperCase());
                typeValueMap.put(rsMetaData.getColumnName(i).toUpperCase(), rsMetaData.getColumnType(i));
            }
            avroWriter = getAvroWriter(typeValueMap, hbase_name, midName, pageNum);
            List<String> allColumnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
            writeHeader(writer, allColumnList);
            StringBuilder sb_ = new StringBuilder();
            List<Object> line = new ArrayList<>(allColumnList.size() + 1);
            String operate = tableBean.getOperate();
            while (resultSet.next()) {
                line.add(operate);
                counter++;
                for (String column_name : allColumnList) {
                    if (queryColumnList.contains(column_name)) {
                        getOneColumnValue(avroWriter, counter, pageNum, resultSet, typeValueMap.get(column_name), sb_, column_name, hbase_name, midName);
                        line.add(sb_.toString());
                        sb_.delete(0, sb_.length());
                    }
                }
                writer.write(line);
                if (counter % JobConstant.BUFFER_ROW == 0) {
                    log.info("表 : " + hbase_name + ", 正在写入文件，已写入" + counter + "行");
                    writer.flush();
                }
                line.clear();
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

    private void writeHeader(CsvListWriter csvListWriter, List<String> columnMetaInfoList) throws Exception {
        if (IsFlag.Shi.getCode().equals(dataExtractionDef.getIs_header()) && writeHeaderFlag) {
            columnMetaInfoList.add(0, "operate");
            csvListWriter.write(columnMetaInfoList);
        }
    }
}
