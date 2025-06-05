package hyren.serv6.hadoop.commons.imp.fileparser.impl;

import fd.ng.db.conf.Dbtype;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.fileparser.FileParserAbstract;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class OrcFileParserDeal extends FileParserAbstract {

    public OrcFileParserDeal(TableBean tableBean, CollectTableBean collectTableBean, String readFile) throws Exception {
        super(tableBean, collectTableBean, readFile);
    }

    @Override
    public String parserFile() {
        RecordReader rows = null;
        long fileRowCount = 0L;
        try (HdfsOperator hdfsOperator = new HdfsOperator()) {
            Reader reader = OrcFile.createReader(new Path(readFile), OrcFile.readerOptions(hdfsOperator.conf));
            rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            List<TypeDescription> children = schema.getChildren();
            VectorizedRowBatch batch = schema.createRowBatch();
            int numberOfChildren = children.size();
            while (rows.nextBatch(batch)) {
                for (int r = 0; r < batch.size; r++) {
                    List<String> valueList = new ArrayList<>();
                    OrcStruct result = new OrcStruct(schema);
                    for (int i = 0; i < numberOfChildren; ++i) {
                        OrcMapredRecordReader.nextValue(batch.cols[i], r, children.get(i), result.getFieldValue(i));
                        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], r, children.get(i), result.getFieldValue(i)));
                    }
                    fileRowCount++;
                    for (int i = 0; i < result.getNumFields(); i++) {
                        valueList.add(getOrcValue(dictionaryTypeList.get(i), result.getFieldValue(i)).toString());
                    }
                    checkData(valueList, fileRowCount);
                    dealLine(valueList);
                }
                writer.flush();
                log.info("正在处理转存文件，已写入" + fileRowCount + "行");
            }
            writer.flush();
        } catch (Exception e) {
            throw new AppSystemException("DB文件采集解析Orc文件失败");
        } finally {
            try {
                if (rows != null)
                    rows.close();
            } catch (IOException e) {
                log.error(String.format("OrcFileParserDeal关闭reader.rows()发生异常! %s", e));
                System.exit(-99);
            }
        }
        return unloadFileAbsolutePath + Constant.METAINFOSPLIT + fileRowCount;
    }

    private Object getOrcValue(String type, WritableComparable<?> tmpValue) {
        Object str;
        type = type.toLowerCase();
        if (type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = tmpValue == null ? null : Boolean.parseBoolean(tmpValue.toString().trim());
        } else if (type.contains(DataTypeConstant.LONG.getMessage()) || type.contains(DataTypeConstant.INT.getMessage()) || type.contains(DataTypeConstant.FLOAT.getMessage()) || type.contains(DataTypeConstant.DOUBLE.getMessage()) || type.contains(DataTypeConstant.DECIMAL.getMessage()) || type.contains(DataTypeConstant.NUMERIC.getMessage())) {
            str = tmpValue == null ? null : new BigDecimal(tmpValue.toString().trim());
        } else {
            str = tmpValue == null ? "" : tmpValue.toString();
        }
        return str;
    }
}
