package hyren.serv6.agent.job.biz.core.dfstage.service;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.utils.InvokeMethod;
import hyren.serv6.base.codes.DataBaseCode;
import hyren.serv6.base.codes.FileFormat;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.hadoop.i.IHadoopFile;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import org.supercsv.io.CsvListReader;
import org.supercsv.prefs.CsvPreference;
import java.io.*;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/12/17 15:43")
public class ReadFileToDataBase implements Callable<Long> {

    private final String fileAbsolutePath;

    private final CollectTableBean collectTableBean;

    private final TableBean tableBean;

    private final DataStoreConfBean dataStoreConfBean;

    private final String lobsFileAbsolutePath;

    public ReadFileToDataBase(String fileAbsolutePath, TableBean tableBean, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean) {
        this.fileAbsolutePath = fileAbsolutePath;
        this.collectTableBean = collectTableBean;
        this.dataStoreConfBean = dataStoreConfBean;
        this.tableBean = tableBean;
        this.lobsFileAbsolutePath = FileNameUtils.getFullPath(fileAbsolutePath) + "LOBS" + File.separator;
    }

    @Method(desc = "", logicStep = "")
    @Override
    public Long call() {
        long count;
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr())) {
            List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
            List<String> sourceTypeList = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
            String tableName = TableNameUtil.getUnderline1TableName(collectTableBean.getStorage_table_name(), collectTableBean.getStorage_type(), collectTableBean.getStorage_time());
            String batchSql = getBatchSql(columnList, tableName, db.getDbtype());
            String file_code = tableBean.getFile_code();
            String file_format = tableBean.getFile_format();
            String column_separator = tableBean.getColumn_separator();
            String is_header = tableBean.getIs_header();
            log.info("=======数据文件加载的数据库是: {}=======", db.getDbtype());
            if (FileFormat.CSV.getCode().equals(file_format)) {
                count = readCsvToDataBase(db, columnList, sourceTypeList, batchSql, file_code, is_header);
            } else if (FileFormat.PARQUET.getCode().equals(file_format)) {
                IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
                count = iHadoopFile.readParquetToDataBase(db, columnList, sourceTypeList, batchSql, fileAbsolutePath, tableBean);
            } else if (FileFormat.ORC.getCode().equals(file_format)) {
                IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
                count = iHadoopFile.readOrcToDataBase(db, sourceTypeList, batchSql, fileAbsolutePath, tableBean);
            } else if (FileFormat.SEQUENCEFILE.getCode().equals(file_format)) {
                IHadoopFile iHadoopFile = ClassBase.hadoopFileInstance();
                count = iHadoopFile.readSequenceToDataBase(db, columnList, sourceTypeList, batchSql, fileAbsolutePath, tableBean);
            } else if (FileFormat.DingChang.getCode().equals(file_format)) {
                if (StringUtil.isEmpty(column_separator)) {
                    count = readDingChangToDataBase(db, columnList, sourceTypeList, batchSql, file_code, is_header);
                } else {
                    count = readFeiDingChangToDataBase(db, columnList, sourceTypeList, batchSql, column_separator, file_code, is_header);
                }
            } else if (FileFormat.FeiDingChang.getCode().equals(file_format)) {
                count = readFeiDingChangToDataBase(db, columnList, sourceTypeList, batchSql, column_separator, file_code, is_header);
            } else {
                throw new AppSystemException("不支持的卸数文件格式");
            }
            db.commit();
        } catch (Exception e) {
            log.error("数据库采集读文件上传到数据库异常", e);
            throw new AppSystemException("数据库采集读文件上传到数据库异常", e);
        }
        return count;
    }

    private long readFeiDingChangToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String dataDelimiter, String database_code, String is_header) {
        long num = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(fileAbsolutePath)), DataBaseCode.ofValueByCode(database_code)))) {
            List<Object[]> pool = new ArrayList<>();
            String line;
            if (IsFlag.Shi.getCode().equals(is_header)) {
                line = reader.readLine();
                if (line != null) {
                    log.info("读取到表头为：" + line);
                }
            }
            List<Object> objs;
            int columnSize = columnSize(columnList);
            List<String> valueList;
            while ((line = reader.readLine()) != null) {
                objs = new ArrayList<>();
                num++;
                valueList = StringUtil.split(line, dataDelimiter);
                if (valueList.size() != columnSize) {
                    throw new AppSystemException("数据文件行: " + num + " ,取到数据的列的数量跟数据字典定义的列的长度不一致，请检查数据是否有问题：" + valueList);
                }
                addColumnData(db, typeList, valueList, objs, columnSize);
                pool.add(objs.toArray());
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(batchSql, pool, num, db);
                }
            }
            if (pool.size() != 0) {
                doBatch(batchSql, pool, num, db);
            }
        } catch (Exception e) {
            throw new AppSystemException("bash插入数据库失败:", e);
        }
        return num;
    }

    public static List<String> getDingChangValueList(String line, List<Integer> lengthList, String database_code) throws Exception {
        List<String> valueList = new ArrayList<>();
        byte[] bytes = line.getBytes(database_code);
        int begin = 0;
        for (int length : lengthList) {
            byte[] byteTmp = new byte[length];
            System.arraycopy(bytes, begin, byteTmp, 0, length);
            begin += length;
            valueList.add(new String(byteTmp, database_code));
        }
        return valueList;
    }

    private long readDingChangToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String database_code, String is_header) {
        database_code = DataBaseCode.ofValueByCode(database_code);
        long num = 0;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(fileAbsolutePath)), database_code))) {
            List<Object[]> pool = new ArrayList<>();
            String line;
            List<String> lengthStrList = StringUtil.split(tableBean.getColLengthInfo(), Constant.METAINFOSPLIT);
            List<Integer> lengthList = new ArrayList<>();
            for (String lengthStr : lengthStrList) {
                lengthList.add(Integer.parseInt(lengthStr));
            }
            if (IsFlag.Shi.getCode().equals(is_header)) {
                line = reader.readLine();
                if (line != null) {
                    log.info("读取到表头为：" + line);
                }
            }
            List<String> valueList;
            List<Object> objs;
            int columnSize = columnSize(columnList);
            while ((line = reader.readLine()) != null) {
                objs = new ArrayList<>();
                num++;
                valueList = getDingChangValueList(line, lengthList, database_code);
                if (valueList.size() != columnSize) {
                    throw new AppSystemException("取到数据的列的数量跟数据字典定义的列的长度不一致，请检查数据是否有问题：========" + valueList);
                }
                addColumnData(db, typeList, valueList, objs, columnSize);
                pool.add(objs.toArray());
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(batchSql, pool, num, db);
                }
            }
            if (pool.size() != 0) {
                doBatch(batchSql, pool, num, db);
            }
        } catch (Exception e) {
            throw new AppSystemException("bash插入数据库失败", e);
        }
        return num;
    }

    private long readCsvToDataBase(DatabaseWrapper db, List<String> columnList, List<String> typeList, String batchSql, String database_code, String is_header) {
        long num = 0;
        String column = null;
        String columnType = null;
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(Files.newInputStream(Paths.get(fileAbsolutePath)), DataBaseCode.ofValueByCode(database_code)));
            CsvListReader csvReader = new CsvListReader(reader, CsvPreference.EXCEL_PREFERENCE)) {
            List<Object[]> pool = new ArrayList<>();
            List<String> lineList;
            if (IsFlag.Shi.getCode().equals(is_header)) {
                lineList = csvReader.read();
                if (lineList != null) {
                    log.info("读取到表头为：" + lineList);
                }
            }
            List<Object> objs;
            int columnSize = columnSize(columnList);
            while ((lineList = csvReader.read()) != null) {
                objs = new ArrayList<>();
                num++;
                for (int j = 0; j < columnSize; j++) {
                    column = columnList.get(j);
                    columnType = typeList.get(j);
                    objs.add(getValue(typeList.get(j), lineList.get(j), db.getDbtype()));
                }
                addMd5Val(objs);
                pool.add(objs.toArray());
                if (num % JobConstant.BUFFER_ROW == 0) {
                    doBatch(batchSql, pool, num, db);
                }
            }
            if (pool.size() != 0) {
                doBatch(batchSql, pool, num, db);
            }
        } catch (Exception e) {
            throw new AppSystemException("bash插入数据库失败, 列字段是: " + column + " 获取的字段类型是: " + columnType, e);
        }
        return num;
    }

    private Object getValue(String type, String tmpValue, Dbtype dbtype) throws IOException {
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

    private void doBatch(String batchSql, List<Object[]> pool, long num, DatabaseWrapper db) {
        int[] ints = db.execBatch(batchSql, pool);
        log.info("本次batch插入" + ints.length);
        log.info("数据库已插入" + num + "条！");
        pool.clear();
    }

    public static String getBatchSql(List<String> columns, String todayTableName, Dbtype dbType) {
        StringBuilder sbAdd = new StringBuilder();
        sbAdd.append("insert into ").append(todayTableName).append("(");
        columns = dbType.ofEscapedkey(columns);
        for (String column : columns) {
            sbAdd.append(column).append(",");
        }
        sbAdd.deleteCharAt(sbAdd.length() - 1);
        sbAdd.append(") values(");
        for (int i = 0; i < columns.size(); i++) {
            if (i != columns.size() - 1) {
                sbAdd.append("?").append(",");
            } else {
                sbAdd.append("?");
            }
        }
        sbAdd.append(")");
        return sbAdd.toString();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "delimiter", desc = "", range = "")
    @Param(name = "fileEncoding", desc = "", range = "")
    private long copyFileDataToKingBase(Connection conn, String tableName, String delimiter, String fileEncoding, String file_format) throws Exception {
        FileFormat fileFormat = FileFormat.ofEnumByCode(file_format);
        if (fileFormat != FileFormat.FeiDingChang) {
            delimiter = Constant.SQLDELIMITER;
        }
        String executeSql = String.format("copy %s from stdin" + " WITH DELIMITER '%s' ENCODING '%s' ", tableName, delimiter, fileEncoding);
        log.info("执行人大的入库SQL: {},表名是: {}, 数据分隔符是: {}, 数据文件编码是: {}, 数据文件路径是: {}, 开始时间: {}", executeSql, tableName, delimiter, fileEncoding, fileAbsolutePath, DateUtil.getDateTime(DateUtil.DATETIME_ZHCN));
        long tableCount = InvokeMethod.executeKingBaseCopyIn(conn, executeSql, fileAbsolutePath);
        log.info("表: {}, 入库执行结束时间是: {}, 总条数是: {}", tableName, DateUtil.getDateTime(DateUtil.DATETIME_ZHCN), tableCount);
        return tableCount;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "columns", desc = "", range = "")
    @Return(desc = "", range = "")
    private int columnSize(List<String> columns) {
        if (tableBean.getAppendMd5()) {
            return columns.size() - 1;
        }
        return columns.size();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "objs", desc = "", range = "")
    private void addMd5Val(List<Object> objs) {
        if (tableBean.getAppendMd5()) {
            objs.add(MD5Util.md5String(StringUtil.join(objs, "")));
        }
    }

    private void addColumnData(DatabaseWrapper db, List<String> typeList, List<String> valueList, List<Object> objs, int columnSize) throws IOException {
        for (int j = 0; j < columnSize; j++) {
            objs.add(getValue(typeList.get(j), valueList.get(j), db.getDbtype()));
        }
        addMd5Val(objs);
    }
}
