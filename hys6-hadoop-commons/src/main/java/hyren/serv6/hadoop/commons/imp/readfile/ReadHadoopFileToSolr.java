package hyren.serv6.hadoop.commons.imp.readfile;

import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.hadoop.commons.hadoop_helper.HdfsOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.RecordReader;
import org.apache.orc.TypeDescription;
import org.apache.orc.mapred.OrcMapredRecordReader;
import org.apache.orc.mapred.OrcStruct;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.common.SolrInputDocument;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class ReadHadoopFileToSolr {

    private final CollectTableBean collectTableBean;

    private final String fileAbsolutePath;

    private final boolean is_append;

    private final String etl_date;

    public ReadHadoopFileToSolr(CollectTableBean collectTableBean, String fileAbsolutePath) {
        this.collectTableBean = collectTableBean;
        this.fileAbsolutePath = fileAbsolutePath;
        this.is_append = StorageType.ZhuiJia.getCode().equals(collectTableBean.getStorage_type());
        this.etl_date = collectTableBean.getEtlDate();
    }

    public long readParquetToSolr(SolrClient solrClient, List<String> columnList, List<String> typeList, boolean isMd5) {
        ParquetReader<Group> build = null;
        try {
            long num = 0;
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(fileAbsolutePath));
            build = reader.build();
            Group line;
            List<SolrInputDocument> docs = new ArrayList<>();
            while ((line = build.read()) != null) {
                num++;
                SolrInputDocument doc = new SolrInputDocument();
                StringBuilder sb = new StringBuilder();
                String storageTableName = collectTableBean.getStorage_table_name();
                doc.addField("table-name", storageTableName);
                for (int j = 0; j < columnList.size(); j++) {
                    if (isMd5) {
                        Object value = getParquetValue(typeList.get(j), line, columnList.get(j));
                        sb.append(value);
                        doc.addField("tf-" + columnList.get(j), value);
                    } else {
                        if (Constant._HYREN_MD5_VAL.equalsIgnoreCase(columnList.get(j))) {
                            if (is_append) {
                                doc.addField("id", storageTableName + "_" + getParquetValue(typeList.get(j), line, columnList.get(j)) + "_" + etl_date);
                            } else {
                                doc.addField("id", storageTableName + "_" + getParquetValue(typeList.get(j), line, columnList.get(j)));
                            }
                        } else {
                            doc.addField("tf-" + columnList.get(j), getParquetValue(typeList.get(j), line, columnList.get(j)));
                        }
                    }
                }
                if (isMd5) {
                    if (is_append) {
                        doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()) + "_" + etl_date);
                    } else {
                        doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()));
                    }
                }
                docs.add(doc);
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(solrClient, docs, num);
                }
            }
            if (!docs.isEmpty()) {
                doBatch(solrClient, docs, num);
            }
            return num;
        } catch (Exception e) {
            throw new AppSystemException("读取parquet文件失败", e);
        } finally {
            try {
                if (build != null)
                    build.close();
            } catch (IOException e) {
                log.error(String.format("关闭 ParquetReader 对象发生异常! %s", e));
                System.exit(-99);
            }
        }
    }

    private static Object getParquetValue(String type, Group line, String column) {
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

    public long readOrcToSolr(SolrClient solrClient, List<String> columnList, List<String> typeList, boolean isMd5) {
        RecordReader rows = null;
        long num = 0L;
        try (HdfsOperator hdfsOperator = new HdfsOperator()) {
            Reader reader = OrcFile.createReader(new Path(fileAbsolutePath), OrcFile.readerOptions(hdfsOperator.conf));
            rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            List<TypeDescription> children = schema.getChildren();
            VectorizedRowBatch batch = schema.createRowBatch();
            int numberOfChildren = children.size();
            List<SolrInputDocument> docs = new ArrayList<>();
            while (rows.nextBatch(batch)) {
                for (int r = 0; r < batch.size; r++) {
                    OrcStruct result = new OrcStruct(schema);
                    for (int i = 0; i < numberOfChildren; ++i) {
                        OrcMapredRecordReader.nextValue(batch.cols[i], r, children.get(i), result.getFieldValue(i));
                        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], r, children.get(i), result.getFieldValue(i)));
                    }
                    num++;
                    SolrInputDocument doc = new SolrInputDocument();
                    StringBuilder sb = new StringBuilder();
                    String storageTableName = collectTableBean.getStorage_table_name();
                    doc.addField("table-name", storageTableName);
                    for (int i = 0; i < result.getNumFields(); i++) {
                        if (isMd5) {
                            Object value = getOrcValue(typeList.get(i), result.getFieldValue(i));
                            sb.append(value);
                            doc.addField("tf-" + columnList.get(i), value);
                        } else {
                            if (Constant._HYREN_MD5_VAL.equalsIgnoreCase(columnList.get(i))) {
                                if (is_append) {
                                    doc.addField("id", storageTableName + "_" + getOrcValue(typeList.get(i), result.getFieldValue(i)) + "_" + etl_date);
                                } else {
                                    doc.addField("id", storageTableName + "_" + getOrcValue(typeList.get(i), result.getFieldValue(i)));
                                }
                            } else {
                                doc.addField("tf-" + columnList.get(i), getOrcValue(typeList.get(i), result.getFieldValue(i)));
                            }
                        }
                    }
                    if (isMd5) {
                        if (is_append) {
                            doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()) + "_" + etl_date);
                        } else {
                            doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()));
                        }
                    }
                    docs.add(doc);
                    if (num % JobConstant.BUFFER_ROW == 0) {
                        doBatch(solrClient, docs, num);
                    }
                }
                if (!docs.isEmpty()) {
                    doBatch(solrClient, docs, num);
                }
            }
            if (!docs.isEmpty()) {
                doBatch(solrClient, docs, num);
            }
        } catch (IOException e) {
            throw new AppSystemException(String.format("读取Orc文件入Solr时发生IO异常! %s", e));
        } finally {
            if (rows != null) {
                try {
                    rows.close();
                } catch (IOException e) {
                    log.error(String.format("关闭 reader.rows() 发生异常! %s", e));
                    System.exit(-99);
                }
            }
        }
        return num;
    }

    private static Object getOrcValue(String type, WritableComparable<?> tmpValue) {
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

    public long readSequenceToSolr(SolrClient server, List<String> columnList, List<String> typeList, boolean isMd5) {
        HdfsOperator hdfsOperator = new HdfsOperator();
        hdfsOperator.conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        hdfsOperator.conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        hdfsOperator.conf.set("fs.defaultFS", "file:///");
        long num = 0L;
        SequenceFile.Reader sfr = null;
        try {
            SequenceFile.Reader.Option optionFile = SequenceFile.Reader.file((new Path(fileAbsolutePath)));
            sfr = new SequenceFile.Reader(hdfsOperator.conf, optionFile);
            NullWritable key = NullWritable.get();
            Text value = new Text();
            List<SolrInputDocument> docs = new ArrayList<>();
            while (sfr.next(key, value)) {
                String str = value.toString();
                List<String> valueList = StringUtil.split(str, Constant.SEQUENCEDELIMITER);
                num++;
                batchToSolr(server, docs, columnList, typeList, valueList, isMd5, num);
            }
            if (!docs.isEmpty()) {
                doBatch(server, docs, num);
            }
        } catch (IOException e) {
            throw new AppSystemException(String.format("读取Sequence文件入Solr时发生IO异常! %s", e));
        } finally {
            if (sfr != null) {
                try {
                    sfr.close();
                } catch (IOException e) {
                    log.error(String.format("关闭 SequenceFile.Reader 发生异常! %s", e));
                    System.exit(-99);
                }
            }
        }
        return num;
    }

    private void batchToSolr(SolrClient server, List<SolrInputDocument> docs, List<String> columnList, List<String> typeList, List<String> valueList, boolean isMd5, long num) {
        SolrInputDocument doc = new SolrInputDocument();
        StringBuilder sb = new StringBuilder();
        String storageTableName = collectTableBean.getStorage_table_name();
        doc.addField("table-name", storageTableName);
        for (int j = 0; j < columnList.size(); j++) {
            if (isMd5) {
                Object value = getOrDefaultValue(typeList.get(j), valueList.get(j));
                sb.append(value);
                doc.addField("tf-" + columnList.get(j), value);
            } else {
                if (Constant._HYREN_MD5_VAL.equalsIgnoreCase(columnList.get(j))) {
                    if (is_append) {
                        doc.addField("id", storageTableName + "_" + getOrDefaultValue(typeList.get(j), valueList.get(j)) + "_" + etl_date);
                    } else {
                        doc.addField("id", storageTableName + "_" + getOrDefaultValue(typeList.get(j), valueList.get(j)));
                    }
                } else {
                    doc.addField("tf-" + columnList.get(j), getOrDefaultValue(typeList.get(j), valueList.get(j)));
                }
            }
        }
        if (isMd5) {
            if (is_append) {
                doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()) + "_" + etl_date);
            } else {
                doc.addField("id", storageTableName + "_" + MD5Util.md5String(sb.toString()));
            }
        }
        docs.add(doc);
        if (num % JobConstant.BUFFER_ROW == 0) {
            doBatch(server, docs, num);
        }
    }

    public static Object getOrDefaultValue(String type, String tmpValue) {
        Object str;
        type = type.toLowerCase();
        if (type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = StringUtil.isBlank(tmpValue) ? null : Boolean.parseBoolean(tmpValue.trim());
        } else {
            str = StringUtil.isBlank(tmpValue) ? "" : tmpValue.trim();
        }
        return str;
    }

    private void doBatch(SolrClient solrClient, List<SolrInputDocument> docs, long num) {
        try {
            solrClient.add(docs);
            solrClient.commit();
            log.info("本次batch插入" + docs.size());
            log.info("数据库已插入" + num + "条！");
            docs.clear();
        } catch (Exception e) {
            throw new AppSystemException("提交数据到solr异常", e);
        }
    }
}
