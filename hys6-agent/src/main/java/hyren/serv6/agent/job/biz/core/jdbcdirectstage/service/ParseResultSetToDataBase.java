package hyren.serv6.agent.job.biz.core.jdbcdirectstage.service;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.MD5Util;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.dfstage.service.ReadFileToDataBase;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.fileparser.FileParserAbstract;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.InputStream;
import java.math.BigDecimal;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/12/17 15:43")
public class ParseResultSetToDataBase {

    private final ResultSet resultSet;

    private final CollectTableBean collectTableBean;

    private final TableBean tableBean;

    private final DataStoreConfBean dataStoreConfBean;

    protected String operateDate;

    protected String operateTime;

    protected String user_id;

    private final boolean is_zipper_flag;

    public ParseResultSetToDataBase(ResultSet resultSet, TableBean tableBean, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean) {
        this.resultSet = resultSet;
        this.collectTableBean = collectTableBean;
        this.dataStoreConfBean = dataStoreConfBean;
        this.tableBean = tableBean;
        this.operateDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.operateTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
        this.user_id = String.valueOf(collectTableBean.getUser_id());
        this.is_zipper_flag = IsFlag.Shi.getCode().equals(collectTableBean.getIs_zipper());
    }

    public long parseResultSet() {
        List<String> columnMetaInfoList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr())) {
            log.info("========================================{}======================================", db.getID());
            db.beginTrans();
            String tableName = TableNameUtil.getUnderline1TableName(collectTableBean.getStorage_table_name(), collectTableBean.getStorage_type(), collectTableBean.getStorage_time());
            Dbtype dbType = db.getDbtype();
            if (dbType == Dbtype.KINGBASE) {
                tableName = db.getDatabaseName() + '.' + tableName;
            }
            String batchSql = ReadFileToDataBase.getBatchSql(columnMetaInfoList, tableName, dbType);
            log.info("连接配置为：" + dataStoreConfBean.getData_store_connect_attr().toString());
            List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
            StringBuilder sbColumn = new StringBuilder();
            selectColumnList = dbType.ofEscapedkey(selectColumnList);
            for (String column : selectColumnList) {
                sbColumn.append(column).append(",");
            }
            sbColumn.deleteCharAt(sbColumn.length() - 1);
            List<String> typeList = StringUtil.split(tableBean.getAllType(), Constant.METAINFOSPLIT);
            int numberOfColumns = selectColumnList.size();
            boolean isodps = collectTableBean.getDb_type() == Dbtype.ODPS;
            log.info("type : " + typeList.size() + "  colName " + numberOfColumns + "   是否为ODPS " + isodps);
            String collectSQL = "select " + sbColumn + " from " + tableName + " where 1 = 2";
            if (dbType != Dbtype.POSTGRESQL) {
                if (dbType == Dbtype.DB2V1 || dbType == Dbtype.DB2V2) {
                    DatabaseMetaData metaData = db.getConnection().getMetaData();
                    String database_name = dbType.getDatabase(db, metaData);
                    collectSQL = "select " + sbColumn + " from " + database_name + "." + tableName + " where 1 = 2";
                }
            }
            log.info("===========collectSQL===============" + collectSQL);
            log.info("===========batchSql===============" + batchSql);
            int[] tarTypeArray = getTarTypeArray(db, collectSQL);
            long counter = 0;
            if (isodps) {
                counter = addODPSData(db, batchSql, tableName, tarTypeArray);
            } else {
                counter = addOtherData(db, batchSql, tableName, tarTypeArray);
            }
            db.commit();
            return counter;
        } catch (Exception e) {
            log.error("batch入库失败", e);
            throw new AppSystemException("数据库直连采集batch入库失败", e);
        }
    }

    private void appendOperateInfo(List list, int numberOfColumns) {
        StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
        if (JobConstant.ISADDOPERATEINFO) {
            if (is_zipper_flag) {
                list.add(numberOfColumns + 3, operateDate);
                list.add(numberOfColumns + 4, operateTime);
                list.add(numberOfColumns + 5, user_id);
            } else if ((storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) && IsFlag.ofEnumByCode(collectTableBean.getIs_md5()) == IsFlag.Shi) {
                list.add(numberOfColumns + 2, operateDate);
                list.add(numberOfColumns + 3, operateTime);
                list.add(numberOfColumns + 4, user_id);
            } else {
                list.add(numberOfColumns + 1, operateDate);
                list.add(numberOfColumns + 2, operateTime);
                list.add(numberOfColumns + 3, user_id);
            }
        }
    }

    private long addODPSData(DatabaseWrapper db, String batchSql, String tableName, int[] tarTypeArray) throws Exception {
        Map<String, Boolean> isZipperFieldInfo = FileParserAbstract.transMd5ColMap(tableBean.getIsZipperFieldInfo());
        StringBuilder md5Value = new StringBuilder();
        String etlDate = collectTableBean.getEtlDate();
        long counter = 0;
        List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
        log.info("要获取值的列信息: {}", selectColumnList);
        int numberOfColumns = selectColumnList.size();
        List<List<Object>> pool = new ArrayList<List<Object>>();
        List<Object> list;
        int cloumnDataIndex;
        while (resultSet.next()) {
            counter++;
            list = new ArrayList<>();
            for (int i = 0; i < numberOfColumns; i++) {
                cloumnDataIndex = i + 1;
                int tarType = tarTypeArray[i];
                if (tarType == Types.BLOB || tarType == Types.LONGVARBINARY) {
                    list.add(i, resultSet.getBlob(cloumnDataIndex));
                } else if (tarType == Types.CLOB) {
                    list.add(i, resultSet.getClob(cloumnDataIndex));
                } else if (tarType == Types.VARBINARY) {
                    list.add(i, resultSet.getBinaryStream(cloumnDataIndex));
                } else {
                    if (tarType == Types.DATE) {
                        list.add(i, resultSet.getDate(cloumnDataIndex));
                    } else if (tarType == Types.TIME) {
                        list.add(i, resultSet.getTime(cloumnDataIndex));
                    } else if (tarType == Types.TIMESTAMP) {
                        list.add(i, resultSet.getTimestamp(cloumnDataIndex));
                    } else if (tarType == Types.STRUCT) {
                        list.add(i, resultSet.getObject(cloumnDataIndex));
                    } else if (tarType == Types.VARCHAR || tarType == Types.CHAR || tarType == Types.NCHAR || tarType == Types.NVARCHAR) {
                        list.add(i, resultSet.getString(cloumnDataIndex));
                    } else if (tarType == Types.INTEGER || tarType == Types.BIGINT || tarType == Types.DECIMAL || tarType == Types.DOUBLE || tarType == Types.FLOAT || tarType == Types.SMALLINT) {
                        list.add(i, resultSet.getBigDecimal(cloumnDataIndex));
                    } else {
                        list.add(i, resultSet.getObject(cloumnDataIndex));
                    }
                }
                if (is_zipper_flag && isZipperFieldInfo.get(selectColumnList.get(i))) {
                    md5Value.append(resultSet.getObject(cloumnDataIndex));
                }
            }
            list.add(numberOfColumns, etlDate);
            if (is_zipper_flag) {
                list.add(numberOfColumns + 1, Constant._MAX_DATE_8);
                list.add(numberOfColumns + 2, MD5Util.md5String(md5Value.toString()));
                md5Value.delete(0, md5Value.length());
            } else if (IsFlag.ofEnumByCode(collectTableBean.getIs_md5()) == IsFlag.Shi) {
                StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
                if (storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) {
                    list.add(numberOfColumns + 1, MD5Util.md5String(md5Value.toString()));
                    md5Value.delete(0, md5Value.length());
                }
            }
            appendOperateInfo(list, numberOfColumns);
            pool.add(list);
            if (counter % JobConstant.BUFFER_ROW == 0) {
                log.info("表名：" + tableName + "正在入库，已batch插入" + counter + "行");
                log.info("表名：" + tableName + " , Batch {} 开始执行时间: {}", JobConstant.BUFFER_ROW, DateUtil.getDateTime());
                boolean flag = executeBatch(db, batchSql, pool);
                if (!flag) {
                    log.info("批量插入数据出现错误,退出");
                }
                pool.clear();
                log.info("表名：" + tableName + " ,Batch 结束执行时间: {}", DateUtil.getDateTime());
            }
        }
        if (pool.size() != 0) {
            boolean flag = executeBatch(db, batchSql, pool);
            if (!flag) {
                log.info("批量插入数据出现错误,退出");
            }
            pool.clear();
        }
        return counter;
    }

    private long addOtherData(DatabaseWrapper db, String batchSql, String tableName, int[] tarTypeArray) throws Exception {
        Map<String, Boolean> isZipperFieldInfo = FileParserAbstract.transMd5ColMap(tableBean.getIsZipperFieldInfo());
        StringBuilder md5Value = new StringBuilder();
        String etlDate = collectTableBean.getEtlDate();
        long counter = 0;
        List<String> selectColumnList = StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT);
        log.info("要获取值的列信息: {}", selectColumnList);
        int[] typeArray = tableBean.getTypeArray();
        int numberOfColumns = selectColumnList.size();
        List<List<Object>> pool = new ArrayList<List<Object>>();
        List<Object> listData;
        Object obj;
        while (resultSet.next()) {
            counter++;
            listData = new ArrayList<>();
            for (int i = 0; i < numberOfColumns; i++) {
                int tarType = tarTypeArray[i];
                if (tarType == Types.BLOB || tarType == Types.LONGVARBINARY) {
                    listData.add(i, resultSet.getBlob(selectColumnList.get(i)));
                } else if (tarType == Types.CLOB) {
                    listData.add(i, resultSet.getClob(selectColumnList.get(i)));
                } else if (tarType == Types.VARBINARY) {
                    listData.add(i, resultSet.getBinaryStream(selectColumnList.get(i)));
                } else {
                    if (tarType == Types.DATE) {
                        listData.add(i, resultSet.getDate(selectColumnList.get(i)));
                    } else if (tarType == Types.TIME) {
                        listData.add(i, resultSet.getTime(selectColumnList.get(i)));
                    } else if (tarType == Types.TIMESTAMP) {
                        listData.add(i, resultSet.getTimestamp(selectColumnList.get(i)));
                    } else if (tarType == Types.STRUCT) {
                        listData.add(i, resultSet.getObject(selectColumnList.get(i)));
                    } else if (tarType == Types.VARCHAR || tarType == Types.CHAR || tarType == Types.NCHAR || tarType == Types.NVARCHAR) {
                        listData.add(i, resultSet.getString(selectColumnList.get(i)));
                    } else if (tarType == Types.BOOLEAN) {
                        listData.add(i, resultSet.getBoolean(selectColumnList.get(i)));
                    } else if (tarType == Types.DECIMAL) {
                        listData.add(i, resultSet.getBigDecimal(selectColumnList.get(i)));
                    } else if (tarType == Types.DOUBLE) {
                        listData.add(i, resultSet.getDouble(selectColumnList.get(i)));
                    } else if (tarType == Types.FLOAT) {
                        listData.add(i, resultSet.getFloat(selectColumnList.get(i)));
                    } else if (tarType == Types.INTEGER || tarType == Types.SMALLINT) {
                        listData.add(i, resultSet.getInt(selectColumnList.get(i)));
                    } else if (tarType == Types.BIGINT) {
                        listData.add(i, resultSet.getLong(selectColumnList.get(i)));
                    } else {
                        listData.add(i, resultSet.getObject(selectColumnList.get(i)));
                    }
                }
                if (is_zipper_flag && isZipperFieldInfo.get(selectColumnList.get(i)) || IsFlag.Shi == IsFlag.ofEnumByCode(collectTableBean.getIs_md5())) {
                    obj = resultSet.getObject(selectColumnList.get(i));
                    if (obj == null) {
                        obj = "";
                    }
                    md5Value.append(obj);
                }
            }
            listData.add(numberOfColumns, etlDate);
            if (is_zipper_flag) {
                listData.add(numberOfColumns + 1, Constant._MAX_DATE_8);
                listData.add(numberOfColumns + 2, MD5Util.md5String(md5Value.toString()));
                md5Value.delete(0, md5Value.length());
            } else if (IsFlag.ofEnumByCode(collectTableBean.getIs_md5()) == IsFlag.Shi) {
                StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
                if (storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) {
                    listData.add(numberOfColumns + 1, MD5Util.md5String(md5Value.toString()));
                    md5Value.delete(0, md5Value.length());
                }
            }
            appendOperateInfo(listData, numberOfColumns);
            pool.add(listData);
            if (counter % JobConstant.BUFFER_ROW == 0) {
                log.info("表名：" + tableName + " , Batch {} 开始执行时间: {}", JobConstant.BUFFER_ROW, DateUtil.getDateTime());
                boolean flag = executeBatch(db, batchSql, pool);
                if (!flag) {
                    log.info("批量插入数据出现错误,退出");
                }
                pool.clear();
                log.info("表名：" + tableName + " ,Batch 结束执行时间: {}", DateUtil.getDateTime());
            }
        }
        if (pool.size() != 0) {
            boolean flag = executeBatch(db, batchSql, pool);
            if (!flag) {
                log.info("批量插入数据出现错误,退出");
            }
            pool.clear();
        }
        return counter;
    }

    private static boolean executeBatch(DatabaseWrapper db, String psql, List<List<Object>> params) {
        if (psql == null || psql.trim().length() < 10 || params == null || params.size() < 1) {
            throw new AppSystemException("参数不能为空！");
        }
        try (PreparedStatement pst = db.getConnection().prepareStatement(psql)) {
            for (List<Object> listParams : params) {
                for (int i = 0; i < listParams.size(); i++) {
                    Object param = listParams.get(i);
                    if (param == null) {
                        pst.setObject(i + 1, null);
                    } else if (param instanceof Blob) {
                        pst.setBinaryStream(i + 1, ((Blob) param).getBinaryStream());
                    } else if (param instanceof Clob) {
                        pst.setCharacterStream(i + 1, ((Clob) param).getCharacterStream());
                    } else if (param instanceof InputStream) {
                        pst.setBinaryStream(i + 1, (InputStream) param);
                    } else if (param instanceof java.sql.Date) {
                        pst.setDate(i + 1, (java.sql.Date) param);
                    } else if (param instanceof Time) {
                        pst.setTime(i + 1, (Time) param);
                    } else if (param instanceof Timestamp) {
                        pst.setTimestamp(i + 1, (Timestamp) param);
                    } else if (param instanceof String) {
                        pst.setString(i + 1, (String) param);
                    } else if (param instanceof BigDecimal) {
                        pst.setObject(i + 1, param);
                    } else {
                        pst.setObject(i + 1, param);
                    }
                }
                pst.addBatch();
            }
            int[] nums = pst.executeBatch();
            return nums.length == params.size();
        } catch (Exception e) {
            throw new AppSystemException("参数不能为空！", e);
        } finally {
        }
    }

    private static ResultSet getResultSet(String collectSQL, DatabaseWrapper db) {
        ResultSet columnSet;
        try {
            String exeSql = String.format("SELECT * FROM ( %s ) HYREN_WHERE_ALIAS WHERE 1 = 2", collectSQL);
            columnSet = db.queryGetResultSet(exeSql);
        } catch (Exception e) {
            throw new AppSystemException("获取ResultSet异常", e);
        }
        return columnSet;
    }

    public static int[] getTarTypeArray(DatabaseWrapper db, String collectSQL) throws SQLException {
        ResultSet resultSet_store = getResultSet(collectSQL, db);
        ResultSetMetaData rsMetaData = resultSet_store.getMetaData();
        int num = rsMetaData.getColumnCount();
        int[] tarTypeArray = new int[num];
        for (int i = 1; i <= num; i++) {
            tarTypeArray[i - 1] = rsMetaData.getColumnType(i);
        }
        return tarTypeArray;
    }
}
