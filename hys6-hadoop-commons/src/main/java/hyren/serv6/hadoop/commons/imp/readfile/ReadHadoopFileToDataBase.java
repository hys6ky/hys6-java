package hyren.serv6.hadoop.commons.imp.readfile;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.TableBean;
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
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/17 15:43")
@Slf4j
public class ReadHadoopFileToDataBase {

    public static long readSequenceToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception {
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
            List<Object[]> pool = new ArrayList<>();
            List<Object> objs;
            int columnSize = columnSize(columnList, tableBean);
            String str;
            while (sfr.next(key, value)) {
                objs = new ArrayList<>();
                num++;
                str = value.toString();
                List<String> valueList = StringUtil.split(str, Constant.SEQUENCEDELIMITER);
                if (valueList.size() != columnSize) {
                    throw new AppSystemException("取到数据的列的数量跟数据字典定义的列的长度不一致，请检查数据是否有问题：========" + valueList);
                }
                addColumnData(db, typeList, valueList, objs, columnSize, fileAbsolutePath, tableBean);
                pool.add(objs.toArray());
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(batchSql, pool, num, db);
                }
            }
            if (!pool.isEmpty()) {
                doBatch(batchSql, pool, num, db);
            }
        } finally {
            if (sfr != null) {
                sfr.close();
            }
        }
        return num;
    }

    public static long readOrcToDataBase(DatabaseWrapper db, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) throws Exception {
        RecordReader rows = null;
        long num = 0L;
        try (HdfsOperator hdfsOperator = new HdfsOperator()) {
            List<Object[]> pool = new ArrayList<>();
            Reader reader = OrcFile.createReader(new Path(fileAbsolutePath), OrcFile.readerOptions(hdfsOperator.conf));
            rows = reader.rows();
            TypeDescription schema = reader.getSchema();
            List<TypeDescription> children = schema.getChildren();
            VectorizedRowBatch batch = schema.createRowBatch();
            int numberOfChildren = children.size();
            List<Object> objs;
            OrcStruct result;
            while (rows.nextBatch(batch)) {
                num++;
                objs = new ArrayList<>();
                for (int r = 0; r < batch.size; r++) {
                    result = new OrcStruct(schema);
                    for (int i = 0; i < numberOfChildren; ++i) {
                        OrcMapredRecordReader.nextValue(batch.cols[i], r, children.get(i), result.getFieldValue(i));
                        result.setFieldValue(i, OrcMapredRecordReader.nextValue(batch.cols[i], r, children.get(i), result.getFieldValue(i)));
                    }
                    for (int i = 0; i < result.getNumFields(); i++) {
                        objs.add(getValue(typeList.get(i), result.getFieldValue(i), db.getDbtype(), fileAbsolutePath));
                    }
                    addMd5Val(objs, tableBean);
                    pool.add(objs.toArray());
                }
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(batchSql, pool, num, db);
                }
            }
            if (!pool.isEmpty()) {
                doBatch(batchSql, pool, num, db);
            }
        } finally {
            if (rows != null) {
                rows.close();
            }
        }
        return num;
    }

    public static long readParquetToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String fileAbsolutePath, TableBean tableBean) {
        ParquetReader<Group> build = null;
        try {
            long num = 0;
            GroupReadSupport readSupport = new GroupReadSupport();
            ParquetReader.Builder<Group> reader = ParquetReader.builder(readSupport, new Path(fileAbsolutePath));
            build = reader.build();
            Group line;
            List<Object[]> pool = new ArrayList<>();
            List<Object> objs;
            int columnSize = columnSize(columnList, tableBean);
            while ((line = build.read()) != null) {
                objs = new ArrayList<>();
                num++;
                for (int j = 0; j < columnSize; j++) {
                    objs.add(getParquetValue(typeList.get(j), line, columnList.get(j), fileAbsolutePath));
                }
                addMd5Val(objs, tableBean);
                pool.add(objs.toArray());
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(batchSql, pool, num, db);
                }
            }
            if (!pool.isEmpty()) {
                doBatch(batchSql, pool, num, db);
            }
            return num;
        } catch (Exception e) {
            throw new AppSystemException("读取parquet文件失败", e);
        } finally {
            if (build != null) {
                try {
                    build.close();
                } catch (Exception e) {
                    log.error("关闭 ParquetReader 的builder对象发生异常!" + e);
                }
            }
        }
    }

    public static Object getParquetValue(String type, Group line, String column, String fileAbsolutePath) throws IOException {
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
        } else if (DataTypeConstant.CLOB.getMessage().equalsIgnoreCase(type)) {
            str = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(line.getString(column, 0).getBytes())));
        } else if (DataTypeConstant.BLOB.getMessage().equalsIgnoreCase(type)) {
            String tmpValue = line.getString(column, 0);
            if (tmpValue == null || StringUtil.isEmpty(tmpValue)) {
                str = null;
            } else {
                String lobsFileAbsolutePath = FileNameUtils.getFullPath(fileAbsolutePath) + "LOBS" + File.separator;
                str = new BufferedInputStream(Files.newInputStream(Paths.get(lobsFileAbsolutePath + tmpValue)));
            }
        } else {
            if ((str = line.getString(column, 0)) == null) {
                str = "";
            }
        }
        return str;
    }

    public static Object getValue(String type, WritableComparable<?> tmpValue, Dbtype dbtype, String fileAbsolutePath) throws IOException {
        Object str;
        type = type.toLowerCase();
        if (type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = tmpValue == null ? null : Boolean.parseBoolean(tmpValue.toString().trim());
        } else if (type.contains(DataTypeConstant.LONG.getMessage()) || type.contains(DataTypeConstant.INT.getMessage()) || type.contains(DataTypeConstant.FLOAT.getMessage()) || type.contains(DataTypeConstant.DOUBLE.getMessage()) || type.contains(DataTypeConstant.DECIMAL.getMessage()) || type.contains(DataTypeConstant.NUMERIC.getMessage())) {
            str = tmpValue == null ? null : new BigDecimal(tmpValue.toString().trim());
        } else if (DataTypeConstant.CLOB.getMessage().equalsIgnoreCase(type)) {
            String clobValue = tmpValue == null ? "" : tmpValue.toString();
            str = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(clobValue.getBytes())));
        } else if (DataTypeConstant.BLOB.getMessage().equalsIgnoreCase(type)) {
            if (tmpValue == null || StringUtil.isEmpty(tmpValue.toString())) {
                str = null;
            } else {
                String lobsFileAbsolutePath = FileNameUtils.getFullPath(fileAbsolutePath) + "LOBS" + File.separator;
                str = new BufferedInputStream(Files.newInputStream(Paths.get(lobsFileAbsolutePath + tmpValue)));
            }
        } else {
            str = tmpValue == null ? "" : tmpValue.toString();
        }
        if (Dbtype.TERADATA == dbtype) {
            if (str != null) {
                str = String.valueOf(str);
            }
        }
        return str;
    }

    public static Object getValue(String type, String tmpValue, Dbtype dbtype, String fileAbsolutePath) throws IOException {
        Object str;
        type = type.toLowerCase();
        String clobValue;
        if (type.contains(DataTypeConstant.BOOLEAN.getMessage())) {
            str = (StringUtil.isBlank(tmpValue) || "NULL".equalsIgnoreCase(tmpValue)) ? null : Boolean.parseBoolean(tmpValue.trim());
        } else if (type.contains(DataTypeConstant.LONG.getMessage()) || type.contains(DataTypeConstant.INT.getMessage()) || type.contains(DataTypeConstant.FLOAT.getMessage()) || type.contains(DataTypeConstant.DOUBLE.getMessage()) || type.contains(DataTypeConstant.DECIMAL.getMessage()) || type.contains(DataTypeConstant.NUMERIC.getMessage()) || type.contains(DataTypeConstant.NUMBER.getMessage())) {
            str = (StringUtil.isBlank(tmpValue) || "NULL".equalsIgnoreCase(tmpValue)) ? null : new BigDecimal(tmpValue.trim());
        } else if (DataTypeConstant.CLOB.getMessage().equalsIgnoreCase(type)) {
            clobValue = tmpValue == null ? "" : tmpValue;
            str = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(clobValue.getBytes())));
        } else if (DataTypeConstant.BLOB.getMessage().equalsIgnoreCase(type)) {
            if (tmpValue == null || StringUtil.isEmpty(tmpValue)) {
                str = null;
            } else {
                String lobsFileAbsolutePath = FileNameUtils.getFullPath(fileAbsolutePath) + "LOBS" + File.separator;
                str = new BufferedInputStream(Files.newInputStream(Paths.get(lobsFileAbsolutePath + tmpValue)));
            }
        } else if (type.contains(DataTypeConstant.TIMESTAMP.getMessage()) || type.contains(DataTypeConstant.DATETIME.getMessage())) {
            str = (StringUtil.isBlank(tmpValue) || "NULL".equalsIgnoreCase(tmpValue)) ? null : Timestamp.valueOf(tmpValue);
        } else if (type.contains(DataTypeConstant.DATE.getMessage())) {
            str = (StringUtil.isBlank(tmpValue) || "NULL".equalsIgnoreCase(tmpValue)) ? null : Date.valueOf(tmpValue);
        } else {
            str = (StringUtil.isBlank(tmpValue) || "NULL".equalsIgnoreCase(tmpValue)) ? "" : tmpValue.trim();
        }
        if (Dbtype.TERADATA == dbtype) {
            if (str != null) {
                str = String.valueOf(str);
            }
        }
        return str;
    }

    public static void doBatch(String batchSql, List<Object[]> pool, long num, DatabaseWrapper db) {
        int[] ints = db.execBatch(batchSql, pool);
        log.info("本次batch插入" + ints.length);
        log.info("数据库已插入" + num + "条！");
        pool.clear();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columns", desc = "", range = "")
    @Return(desc = "", range = "")
    public static int columnSize(List<String> columns, TableBean tableBean) {
        if (tableBean.getAppendMd5()) {
            return columns.size() - 1;
        }
        return columns.size();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objs", desc = "", range = "")
    public static void addMd5Val(List<Object> objs, TableBean tableBean) {
        if (tableBean.getAppendMd5()) {
            objs.add(MD5Util.md5String(StringUtil.join(objs, "")));
        }
    }

    public static void addColumnData(DatabaseWrapper db, List<String> typeList, List<String> valueList, List<Object> objs, int columnSize, String fileAbsolutePath, TableBean tableBean) throws IOException {
        for (int j = 0; j < columnSize; j++) {
            objs.add(getValue(typeList.get(j), valueList.get(j), db.getDbtype(), fileAbsolutePath));
        }
        addMd5Val(objs, tableBean);
    }
}
