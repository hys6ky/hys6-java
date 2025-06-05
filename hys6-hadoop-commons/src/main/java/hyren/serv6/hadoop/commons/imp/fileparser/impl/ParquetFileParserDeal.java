package hyren.serv6.hadoop.commons.imp.fileparser.impl;

import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.fileparser.FileParserAbstract;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ParquetFileParserDeal extends FileParserAbstract {

    public ParquetFileParserDeal(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        super(tableBean, collectTableBean, readFile);
    }

    @Override
    public String parserFile() {
        ParquetReader<Group> build = null;
        long fileRowCount = 0;
        try {
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(readFile));
            build = reader.build();
            Group line;
            while ((line = build.read()) != null) {
                List<String> valueList = new ArrayList<>();
                fileRowCount++;
                for (int j = 0; j < dictionaryColumnList.size(); j++) {
                    valueList.add(getParquetValue(dictionaryTypeList.get(j), line, dictionaryColumnList.get(j)).toString());
                }
                checkData(valueList, fileRowCount);
                dealLine(valueList);
                if (fileRowCount % JobConstant.BUFFER_ROW == 0) {
                    writer.flush();
                    log.info("正在处理转存文件，已写入" + fileRowCount + "行");
                }
            }
            writer.flush();
        } catch (Exception e) {
            throw new AppSystemException("读取parquet文件失败", e);
        } finally {
            try {
                if (build != null) {
                    build.close();
                }
            } catch (IOException e) {
                log.error(String.format("ParquetFileParserDeal关闭ParquetReader.Builder发生异常! %s", e));
                System.exit(-99);
            }
        }
        return unloadFileAbsolutePath + Constant.METAINFOSPLIT + fileRowCount;
    }

    private Object getParquetValue(String type, Group line, String column) {
        Object str;
        type = type.toLowerCase();
        if (type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = line.getBoolean(column, 0);
        } else if (type.contains(DataTypeConstant.INT8.getMessage()) || type.contains(DataTypeConstant.BIGINT.getMessage()) || type.contains(DataTypeConstant.LONG.getMessage())) {
            str = line.getLong(column, 0);
        } else if (type.contains(DataTypeConstant.INT.getMessage())) {
            str = line.getInteger(column, 0);
        } else if (type.contains(DataTypeConstant.FLOAT.getMessage())) {
            str = line.getFloat(column, 0);
        } else if (type.contains(DataTypeConstant.DOUBLE.getMessage()) || type.contains(DataTypeConstant.DECIMAL.getMessage()) || type.contains(DataTypeConstant.NUMERIC.getMessage())) {
            str = line.getDouble(column, 0);
        } else {
            if ((str = line.getString(column, 0)) == null) {
                str = "";
            }
        }
        return str;
    }
}
