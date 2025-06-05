package hyren.serv6.agent.job.biz.core.dfstage;

import com.jcraft.jsch.JSchException;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.increasement.impl.IncreasementByMpp;
import hyren.serv6.agent.job.biz.utils.*;
import hyren.serv6.base.codes.*;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.hadoop.i.IHbase;
import hyren.serv6.commons.hadoop.sqlutils.HSqlExecute;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.ColUtil;
import hyren.serv6.commons.utils.TypeTransLength;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.DataTypeConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.database.OracleDatabaseCode;
import hyren.serv6.base.utils.jsch.SSHDetails;
import hyren.serv6.base.utils.jsch.SSHOperate;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.io.IOException;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DFDataLoadingStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    public DFDataLoadingStageImpl(CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        String storageTableName = collectTableBean.getStorage_table_name();
        log.info("------------------表" + storageTableName + "DB文件采集数据加载阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.DATALOADING.getCode());
        try {
            if (UnloadType.ZengLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                log.info("表" + storageTableName + "增量卸数数据加载阶段不用做任何操作");
            } else if (UnloadType.QuanLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                List<DataStoreConfBean> dataStoreConfBeanList = collectTableBean.getDataStoreConfBean();
                String todayTableName;
                for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                    todayTableName = TableNameUtil.getUnderline1TableName(storageTableName, collectTableBean.getStorage_type(), collectTableBean.getStorage_time());
                    String hdfsFilePath = DFUploadStageImpl.getUploadHdfsPath(collectTableBean);
                    if (Store_type.DATABASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        if (IsFlag.Shi.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                            log.info("*******************" + todayTableName);
                            createExternalTableLoadData(todayTableName, collectTableBean, dataStoreConfBean, stageParamInfo.getTableBean(), stageParamInfo.getFileNameArr());
                        } else if (IsFlag.Fou.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                            continue;
                        } else {
                            throw new AppSystemException("表" + storageTableName + "错误的是否标识");
                        }
                    } else if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                        if (IsFlag.Shi.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                            createHiveTableLoadData(todayTableName, hdfsFilePath, dataStoreConfBean, stageParamInfo.getTableBean(), collectTableBean);
                        } else if (IsFlag.Fou.getCode().equals(dataStoreConfBean.getIs_hadoopclient())) {
                            continue;
                        } else {
                            throw new AppSystemException("表" + storageTableName + "错误的是否标识");
                        }
                    } else if (Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        bulkloadLoadDataToHbase(todayTableName, hdfsFilePath, dataStoreConfBean, stageParamInfo.getTableBean(), collectTableBean);
                    } else if (Store_type.SOLR.getCode().equals(dataStoreConfBean.getStore_type())) {
                        continue;
                    } else if (Store_type.ElasticSearch.getCode().equals(dataStoreConfBean.getStore_type())) {
                        log.warn("DB文件采集数据加载进ElasticSearch没有实现");
                    } else if (Store_type.MONGODB.getCode().equals(dataStoreConfBean.getStore_type())) {
                        log.warn("DB文件采集数据加载进MONGODB没有实现");
                    } else {
                        throw new AppSystemException("表" + storageTableName + "不支持的存储类型");
                    }
                    log.info("数据成功进入库" + dataStoreConfBean.getDsl_name() + "下的表" + storageTableName);
                }
            } else {
                throw new AppSystemException("表" + storageTableName + "DB文件采集指定的数据抽取卸数方式类型不正确");
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + storageTableName + "DB文件采集数据加载阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + storageTableName + "DB文件采集数据加载阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.DBWenJian.getCode());
        return stageParamInfo;
    }

    public static void bulkloadLoadDataToHbase(String todayTableName, String hdfsFilePath, DataStoreConfBean dataStoreConfBean, TableBean tableBean, CollectTableBean collectTableBean) {
        String isMd5 = IsFlag.Fou.getCode();
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        StringBuilder rowKeyIndex = new StringBuilder();
        Map<String, Map<Integer, String>> additInfoFieldMap = dataStoreConfBean.getSortAdditInfoFieldMap();
        if (additInfoFieldMap != null && !additInfoFieldMap.isEmpty()) {
            Map<Integer, String> column_map = additInfoFieldMap.get(StoreLayerAdded.RowKey.getCode());
            if (column_map != null && !column_map.isEmpty()) {
                for (int key : column_map.keySet()) {
                    for (int i = 0; i < columnList.size(); i++) {
                        if (column_map.get(key).equalsIgnoreCase(columnList.get(i))) {
                            rowKeyIndex.append(i).append(Constant.METAINFOSPLIT);
                        }
                    }
                }
            }
        }
        if (rowKeyIndex.length() == 0) {
            if (columnList.contains(Constant._HYREN_MD5_VAL)) {
                for (int i = 0; i < columnList.size(); i++) {
                    if (Constant._HYREN_MD5_VAL.equals(columnList.get(i))) {
                        rowKeyIndex.append(i).append(Constant.METAINFOSPLIT);
                    }
                }
            } else {
                for (int i = 0; i < columnList.size(); i++) {
                    String colName = columnList.get(i);
                    if (!(Constant._HYREN_S_DATE.equals(colName) || Constant._HYREN_E_DATE.equals(colName) || Constant._HYREN_OPER_DATE.equals(colName) || Constant._HYREN_OPER_TIME.equals(colName) || Constant._HYREN_OPER_PERSON.equals(colName))) {
                        rowKeyIndex.append(i).append(Constant.METAINFOSPLIT);
                    }
                }
                isMd5 = IsFlag.Shi.getCode();
            }
        }
        rowKeyIndex.delete(rowKeyIndex.length() - Constant.METAINFOSPLIT.length(), rowKeyIndex.length());
        IHbase iHbase = ClassBase.HbaseInstance();
        iHbase.loadDataToHbase(todayTableName, hdfsFilePath, tableBean, collectTableBean, isMd5, rowKeyIndex, dataStoreConfBean);
    }

    public static void createExternalTableLoadData(String todayTableName, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean, TableBean tableBean, String[] fileNameArr) {
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        String database_type = data_store_connect_attr.get(StorageTypeKey.database_type);
        DatabaseWrapper db = null;
        try (SSHOperate sshOperate = new SSHOperate(new SSHDetails(data_store_connect_attr.get(StorageTypeKey.sftp_host), data_store_connect_attr.get(StorageTypeKey.sftp_user), data_store_connect_attr.get(StorageTypeKey.sftp_pwd), data_store_connect_attr.get(StorageTypeKey.sftp_port)), 0)) {
            db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr());
            String file_format = tableBean.getFile_format();
            if (tableBean.getStorage_time() > 0) {
                backupToDayTable(todayTableName, db);
                createExternalTodayTable(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, database_type, db, sshOperate, file_format);
            } else {
                if (StorageType.TiHuan == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    dropToDayTable(todayTableName, db);
                    createExternalTodayTable(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, database_type, db, sshOperate, file_format);
                } else if (StorageType.ZhuiJia == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    if (db.isExistTable(todayTableName)) {
                        externalAppendTable(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, db, file_format);
                    } else {
                        createExternalTodayTable(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, database_type, db, sshOperate, file_format);
                    }
                } else if (StorageType.ZengLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.QuanLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.LiShiLaLian == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    backupToDayTable(todayTableName, db);
                    createExternalTodayTable(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, database_type, db, sshOperate, file_format);
                }
            }
            clearTemporaryFile(database_type, collectTableBean, data_store_connect_attr.get(StorageTypeKey.external_root_path), sshOperate, fileNameArr);
            clearTemporaryLog(database_type, todayTableName, data_store_connect_attr.get(StorageTypeKey.external_root_path), sshOperate);
            backupPastTable(collectTableBean, db);
        } catch (Exception e) {
            if (db != null) {
                recoverBackupToDayTable(todayTableName, db);
            }
            throw new AppSystemException("表" + collectTableBean.getStorage_table_name() + "执行数据库" + dataStoreConfBean.getDsl_name() + "外部表加载数据的sql报错", e);
        } finally {
            clearTemporaryTable(database_type, fileNameArr, todayTableName, db, dataStoreConfBean.getDsl_name());
            if (db != null)
                db.close();
        }
    }

    private static void externalAppendTable(String todayTableName, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean, TableBean tableBean, String[] fileNameArr, Map<String, String> data_store_connect_attr, DatabaseWrapper db, String file_format) {
        List<String> sqlList = new ArrayList<>();
        sqlList.add("DELETE FROM " + todayTableName + " WHERE " + Constant._HYREN_S_DATE + "='" + collectTableBean.getEtlDate() + "'");
        String tmpTodayTableName = getTmpTodayTableName(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, db, file_format);
        StringBuilder insertDataSql = new StringBuilder(120);
        List<String> columns = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        insertDataSql.append("INSERT INTO ");
        insertDataSql.append(todayTableName);
        insertDataSql.append("(");
        for (String col : columns) {
            insertDataSql.append(col).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" ) ");
        insertDataSql.append(" select ");
        for (String col : columns) {
            insertDataSql.append(tmpTodayTableName).append(".").append(col).append(",");
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" from ");
        insertDataSql.append(tmpTodayTableName);
        sqlList.add(insertDataSql.toString());
        HSqlExecute.executeSql(sqlList, db);
    }

    private static void createExternalTodayTable(String todayTableName, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean, TableBean tableBean, String[] fileNameArr, Map<String, String> data_store_connect_attr, String database_type, DatabaseWrapper db, SSHOperate sshOperate, String file_format) throws IOException, JSchException {
        Dbtype dbType = ConnectionTool.getDbType(database_type);
        if (dbType == Dbtype.ORACLE) {
            String tmpTodayTableName = getTmpTodayTableName(todayTableName, collectTableBean, dataStoreConfBean, tableBean, fileNameArr, data_store_connect_attr, db, file_format);
            IncreasementByMpp.dropTableIfExists(todayTableName, db);
            if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
                HSqlExecute.executeSql(" CREATE TABLE " + todayTableName + " AS SELECT * FROM  " + tmpTodayTableName, db);
            } else {
                HSqlExecute.executeSql(" CREATE TABLE " + todayTableName + " parallel (degree 4) nologging  " + "AS SELECT * FROM  " + tmpTodayTableName, db);
            }
            String bad_files = sshOperate.execCommandBySSH("ls " + data_store_connect_attr.get(StorageTypeKey.external_root_path) + tmpTodayTableName.toUpperCase() + "*bad");
            String storageTableName = collectTableBean.getStorage_table_name();
            if (!StringUtil.isEmpty(bad_files)) {
                throw new AppSystemException("表" + storageTableName + "你所生成的文件无法load到Oracle数据库，请查看数据库服务器下的bad文件" + bad_files + "及其相关错误日志");
            } else {
                log.info("表" + storageTableName + "oracle数据库进数成功");
            }
        } else if (dbType == Dbtype.POSTGRESQL) {
            String uploadServerPath = DFUploadStageImpl.getUploadServerPath(collectTableBean, data_store_connect_attr.get(StorageTypeKey.external_root_path));
            if (FileFormat.CSV.getCode().equals(file_format)) {
                createPostgresqlExternalTable(todayTableName, tableBean, fileNameArr, uploadServerPath, dataStoreConfBean, db);
            } else {
                throw new AppSystemException("表" + collectTableBean.getStorage_table_name() + "Postgresql数据库外部表进数目前只支持Csv文件进数，请在页面选择转存" + "或者使用csv格式文件进行db文件采集直接加载进Postgresql");
            }
        } else {
            throw new AppSystemException(dataStoreConfBean.getDsl_name() + "数据库暂不支持外部表的形式入库");
        }
    }

    private static String getTmpTodayTableName(String todayTableName, CollectTableBean collectTableBean, DataStoreConfBean dataStoreConfBean, TableBean tableBean, String[] fileNameArr, Map<String, String> data_store_connect_attr, DatabaseWrapper db, String file_format) {
        String tmpTodayTableName = TableNameUtil.getTempTableNameSuffixT(todayTableName);
        log.info(todayTableName + "===============" + tmpTodayTableName);
        if (FileFormat.FeiDingChang.getCode().equals(file_format)) {
            IncreasementByMpp.dropTableIfExists(tmpTodayTableName, db);
            String oracleExternalTableSql = createOracleExternalTable(tmpTodayTableName, tableBean, fileNameArr, dataStoreConfBean, data_store_connect_attr.get(StorageTypeKey.external_directory), db);
            HSqlExecute.executeSql(oracleExternalTableSql, db);
        } else {
            throw new AppSystemException("表" + collectTableBean.getStorage_table_name() + "Oracle数据库外部表进数目前只支持非定长文件进数，请在页面选择转存" + "或者使用非定长格式文件进行db文件采集直接加载进Oracle");
        }
        return tmpTodayTableName;
    }

    public static void clearTemporaryFile(String database_type, CollectTableBean collectTableBean, String external_root_path, SSHOperate sshOperate, String[] fileNameArr) {
        String past_hbase_name = fileNameArr[0].split(collectTableBean.getTable_name())[0] + collectTableBean.getTable_name();
        if (FileUtil.isSysDir(external_root_path)) {
            throw new AppSystemException("请不要删除系统目录下的文件" + external_root_path);
        }
        Dbtype dbType = ConnectionTool.getDbType(database_type);
        if (dbType == Dbtype.ORACLE) {
            String lobs_file = "find " + external_root_path + " -name \"LOBs_" + past_hbase_name + "_*\" | xargs rm -rf 'LOBs_" + past_hbase_name + "_*'";
            try {
                sshOperate.execCommandBySSH(lobs_file);
            } catch (Exception e) {
                throw new BusinessException("命令执行失败!" + lobs_file);
            }
        } else if (dbType == Dbtype.POSTGRESQL) {
            external_root_path = DFUploadStageImpl.getUploadServerPath(collectTableBean, external_root_path);
        }
        for (String fileName : fileNameArr) {
            String rm_shell_str = "rm -rf " + external_root_path + fileName;
            try {
                sshOperate.execCommandBySSH(rm_shell_str);
            } catch (Exception e) {
                throw new BusinessException("命令执行失败!" + rm_shell_str);
            }
        }
    }

    public static void clearTemporaryLog(String database_type, String todayTableName, String external_root_path, SSHOperate sshOperate) {
        if (FileUtil.isSysDir(external_root_path)) {
            throw new AppSystemException("请不要删除系统目录下的文件" + external_root_path);
        }
        String tmpTodayTableName = TableNameUtil.getTempTableNameSuffixT(todayTableName);
        Dbtype dbType = ConnectionTool.getDbType(database_type);
        if (dbType == Dbtype.ORACLE) {
            String rm_shell_str = "rm -rf " + external_root_path + tmpTodayTableName.toUpperCase() + "*log";
            try {
                sshOperate.execCommandBySSH(rm_shell_str);
            } catch (Exception e) {
                throw new BusinessException("命令执行失败!" + rm_shell_str);
            }
        }
    }

    public static void clearTemporaryTable(String database_type, String[] fileNameArr, String todayTableName, DatabaseWrapper db, String dsl_name) {
        Dbtype dbType = ConnectionTool.getDbType(database_type);
        if (dbType == Dbtype.ORACLE) {
            IncreasementByMpp.dropTableIfExists(TableNameUtil.getTempTableNameSuffixT(todayTableName), db);
        } else if (dbType == Dbtype.POSTGRESQL) {
            for (int i = 0; i < fileNameArr.length; i++) {
                String table_name = todayTableName + "_tmp" + i;
                IncreasementByMpp.dropTableIfExists(table_name, db);
            }
        } else {
            throw new AppSystemException(dsl_name + "数据库暂不支持外部表的形式入库");
        }
    }

    public static void createPostgresqlExternalTable(String todayTableName, TableBean tableBean, String[] fileNameArr, String uploadServerPath, DataStoreConfBean dataStoreConfBean, DatabaseWrapper db) {
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        List<String> typeList = StringUtil.split(tableBean.getColTypeMetaInfo().toUpperCase(), Constant.METAINFOSPLIT);
        List<String> tarTypes = ColUtil.getTarTypes(tableBean, dataStoreConfBean.getDsl_id());
        boolean is_header = IsFlag.Shi.getCode().equalsIgnoreCase(tableBean.getIs_header());
        StringBuilder insertColumns = new StringBuilder();
        for (String col : columnList) {
            insertColumns.append(col).append(",");
        }
        insertColumns.delete(insertColumns.length() - 1, insertColumns.length());
        IncreasementByMpp.dropTableIfExists(todayTableName, db);
        StringBuilder createTodayTable = new StringBuilder();
        createTodayTable.append("create table ").append(todayTableName);
        createTodayTable.append("(");
        SQLUtil.getSqlCond(columnList, typeList, tarTypes, createTodayTable, db.getDbtype());
        createTodayTable.deleteCharAt(createTodayTable.length() - 1);
        createTodayTable.append(" )");
        HSqlExecute.executeSql(createTodayTable.toString(), db);
        String table_name;
        StringBuilder createExternalTable;
        for (int i = 0; i < fileNameArr.length; i++) {
            table_name = todayTableName + "_tmp" + i;
            IncreasementByMpp.dropTableIfExists(table_name, db);
            createExternalTable = new StringBuilder();
            createExternalTable.append("CREATE FOREIGN TABLE ");
            createExternalTable.append(table_name);
            createExternalTable.append("(");
            for (int j = 0; j < columnList.size(); j++) {
                createExternalTable.append(columnList.get(j)).append(" ").append(typeList.get(j)).append(",");
            }
            createExternalTable.deleteCharAt(createExternalTable.length() - 1);
            createExternalTable.append(") SERVER pg_file_server OPTIONS (filename '");
            createExternalTable.append(uploadServerPath).append(fileNameArr[i]);
            createExternalTable.append("', FORMAT 'csv',header '").append(is_header).append("',DELIMITER '").append(tableBean.getColumn_separator()).append("' ,null '')");
            HSqlExecute.executeSql(createExternalTable.toString(), db);
            HSqlExecute.executeSql("INSERT INTO " + todayTableName + "(" + insertColumns + ") SELECT " + insertColumns + " FROM " + table_name, db);
        }
    }

    public static String createOracleExternalTable(String tmpTodayTableName, TableBean tableBean, String[] fileNameArr, DataStoreConfBean dataStoreConfBean, String external_directory, DatabaseWrapper db) {
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        List<String> typeList = StringUtil.split(tableBean.getColTypeMetaInfo().toUpperCase(), Constant.METAINFOSPLIT);
        List<String> tarTypes = ColUtil.getTarTypes(tableBean, dataStoreConfBean.getDsl_id());
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(tmpTodayTableName);
        sql.append("(");
        SQLUtil.getSqlCond(columnList, typeList, tarTypes, sql, db.getDbtype());
        sql.deleteCharAt(sql.length() - 1);
        sql.append(" )ORGANIZATION external ");
        sql.append(" ( ");
        sql.append(" TYPE ORACLE_LOADER ");
        sql.append(" DEFAULT DIRECTORY ").append(external_directory);
        sql.append(" ACCESS PARAMETERS( ");
        sql.append(" RECORDS DELIMITED BY NEWLINE");
        sql.append(" CHARACTERSET ").append(OracleDatabaseCode.ofValueByCode(tableBean.getFile_code()));
        if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
            sql.append(" SKIP 1");
        }
        sql.append(" FIELDS TERMINATED BY '").append(tableBean.getColumn_separator()).append("' ");
        sql.append(" MISSING FIELD VALUES ARE NULL  ");
        sql.append(getTransformsSqlForLobs(columnList, tarTypes, external_directory));
        sql.append(" ) ");
        sql.append(" LOCATION ");
        sql.append(" ( ");
        for (String fileName : fileNameArr) {
            sql.append("'").append(fileName).append("'").append(",");
        }
        sql.delete(sql.length() - 1, sql.length());
        sql.append(" )) REJECT LIMIT UNLIMITED ");
        return sql.toString();
    }

    public static String getTransformsSqlForLobs(List<String> columns, List<String> types, String external_directory) {
        StringBuilder sb = new StringBuilder(1024);
        log.info("*******************ColumnType: {}*******************", types);
        if (types.contains("BLOB") || types.contains("CLOB")) {
            sb.append("(");
            for (int i = 0; i < types.size(); i++) {
                log.info("====Column Name: {}, ColumnTyoe: {}===", columns.get(i), types.get(i));
                if ("BLOB".equals(types.get(i))) {
                    sb.append(columns.get(i)).append("_hylobs").append(" ").append("CHAR(100)").append(",");
                } else if ("CLOB".equals(types.get(i))) {
                    sb.append(columns.get(i)).append(" ").append("CHAR(1000000)").append(",");
                } else {
                    sb.append(columns.get(i)).append(" ");
                    if (types.get(i).equalsIgnoreCase("DATE") || types.get(i).equalsIgnoreCase("TIMESTAMP")) {
                        sb.append(types.get(i)).append(" 'YYYY-MM-DD HH24:MI:SS'").append(",");
                    } else if (TypeTransLength.getLength(types.get(i)) == null) {
                        sb.append("CHAR(").append(255).append(')').append(',');
                    } else {
                        sb.append("CHAR(").append(TypeTransLength.getLength(types.get(i))).append(')').append(',');
                    }
                }
            }
            sb.deleteCharAt(sb.length() - 1);
            sb.append(")");
            if (types.contains("BLOB")) {
                sb.append(" COLUMN TRANSFORMS ( ");
                for (int i = 0; i < types.size(); i++) {
                    if ("BLOB".equals(types.get(i))) {
                        sb.append(columns.get(i)).append(" from ").append(" LOBFILE (").append(columns.get(i)).append("_hylobs").append(")");
                        sb.append(" from (").append(external_directory).append(")").append(" BLOB ").append(",");
                    }
                }
                sb.deleteCharAt(sb.length() - 1);
                sb.append(" ) ");
            }
        }
        return sb.toString();
    }

    @Override
    public int getStageCode() {
        return StageConstant.DATALOADING.getCode();
    }

    public static void createHiveTableLoadData(String todayTableName, String hdfsFilePath, DataStoreConfBean dataStoreConfBean, TableBean tableBean, CollectTableBean collectTableBean) {
        DatabaseWrapper db = null;
        try {
            db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr());
            if (tableBean.getStorage_time() > 0) {
                backupToDayTable(todayTableName, db);
                createTodayTable(todayTableName, dataStoreConfBean, tableBean, db);
            } else {
                if (StorageType.TiHuan == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    dropToDayTable(todayTableName, db);
                    createTodayTable(todayTableName, dataStoreConfBean, tableBean, db);
                } else if (StorageType.ZhuiJia == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    if (db.isExistTable(todayTableName)) {
                        backupTodayTableByAppend(todayTableName, tableBean, collectTableBean, db);
                    } else {
                        createTodayTable(todayTableName, dataStoreConfBean, tableBean, db);
                    }
                } else if (StorageType.ZengLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.QuanLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.LiShiLaLian == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    backupToDayTable(todayTableName, db);
                    createTodayTable(todayTableName, dataStoreConfBean, tableBean, db);
                }
            }
            HSqlExecute.executeSql("load data inpath '" + hdfsFilePath + "' into table " + todayTableName, db);
            backupPastTable(collectTableBean, db);
        } catch (Exception e) {
            if (db != null) {
                recoverBackupToDayTable(todayTableName, db);
            }
            throw new AppSystemException("执行hive加载数据的sql报错", e);
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

    public static void backupTodayTableByAppend(String todayTableName, TableBean tableBean, CollectTableBean collectTableBean, DatabaseWrapper db) {
        try {
            String etlDate = collectTableBean.getEtlDate();
            ResultSet resultSet = db.queryGetResultSet("select " + Constant._HYREN_S_DATE + " from " + todayTableName + " where " + Constant._HYREN_S_DATE + " = '" + etlDate + "' limit 1");
            log.info(resultSet.next() + "###################haveAppendTodayData####################");
            if (resultSet.next()) {
                List<String> columns = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
                String join = StringUtils.join(columns, ',');
                String sql = "create table " + todayTableName + "_restore as select  " + join + " from " + todayTableName + " where " + Constant._HYREN_S_DATE + "<>'" + etlDate + "'";
                List<String> sqlList = new ArrayList<>();
                sqlList.add(sql);
                sqlList.add("drop table if exists " + todayTableName);
                sqlList.add("alter table " + todayTableName + "_restore rename to " + todayTableName);
                HSqlExecute.executeSql(sqlList, db);
            }
        } catch (Exception e) {
            throw new AppSystemException("数据加载追加错误:" + e.getMessage());
        }
    }

    private static void createTodayTable(String todayTableName, DataStoreConfBean dataStoreConfBean, TableBean tableBean, DatabaseWrapper db) {
        String file_format = tableBean.getFile_format();
        log.info(FileFormat.ofValueByCode(file_format) + "==============todayTableName===================" + todayTableName);
        if (FileFormat.SEQUENCEFILE.getCode().equals(file_format) || FileFormat.PARQUET.getCode().equals(file_format) || FileFormat.ORC.getCode().equals(file_format)) {
            HSqlExecute.executeSql(genHiveLoadColumnar(todayTableName, file_format, dataStoreConfBean.getDsl_id(), tableBean), db);
        } else if (FileFormat.FeiDingChang.getCode().equals(file_format)) {
            HSqlExecute.executeSql(genHiveLoad(todayTableName, dataStoreConfBean.getDsl_id(), tableBean, tableBean.getColumn_separator()), db);
        } else if (FileFormat.CSV.getCode().equals(file_format)) {
            HSqlExecute.executeSql(genHiveLoadCsv(todayTableName, dataStoreConfBean.getDsl_id(), tableBean), db);
        } else if (FileFormat.DingChang.getCode().equals(file_format)) {
            HSqlExecute.executeSql(genHiveDingChangLoad(todayTableName, dataStoreConfBean.getDsl_id(), tableBean, tableBean.getColumn_separator()), db);
        } else {
            throw new AppSystemException("目前仅支持：SequenceFile、Parquet、Orc、Csv、非定长、定长文件；" + "暂不支持其他特殊类型直接加载到hive表");
        }
    }

    public static String genHiveLoadCsv(String todayTableName, long dsl_id, TableBean tableBean) {
        StringBuilder sql = new StringBuilder(120);
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        sql.append("CREATE TABLE IF NOT EXISTS ").append(todayTableName).append(" (");
        List<String> tarTypes = StringUtil.split(tableBean.getTbColTarMap().get(dsl_id), Constant.METAINFOSPLIT);
        List<String> types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        for (int i = 0; i < columnList.size(); i++) {
            if (!Constant.HYRENFIELD.contains(columnList.get(i).toUpperCase())) {
                if (!tarTypes.isEmpty() && StringUtil.isNotBlank(tarTypes.get(i)) && !"NULL".equalsIgnoreCase(tarTypes.get(i))) {
                    sql.append("`").append(columnList.get(i)).append("` ").append(tarTypes.get(i)).append(",");
                } else {
                    sql.append("`").append(columnList.get(i)).append("` ").append(types.get(i)).append(",");
                }
            } else {
                sql.append("`").append(columnList.get(i)).append("` ").append("string").append(",");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde' stored as TEXTFILE");
        if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
            sql.append(" tblproperties (\"skip.header.line.count\"=\"1\")");
        }
        return sql.toString();
    }

    public static String getColumnarFileHiveStored(String fileExtension) {
        if (FileFormat.PARQUET.getCode().equals(fileExtension)) {
            return "parquet";
        } else if (FileFormat.ORC.getCode().equals(fileExtension)) {
            return "orc";
        } else if (FileFormat.SEQUENCEFILE.getCode().equals(fileExtension)) {
            return "sequencefile";
        } else {
            throw new IllegalArgumentException(fileExtension);
        }
    }

    public static String genHiveLoadColumnar(String todayTableName, String file_format, long dsl_id, TableBean tableBean) {
        String hiveStored = getColumnarFileHiveStored(file_format);
        String type;
        StringBuilder sql = new StringBuilder(120);
        sql.append("CREATE TABLE IF NOT EXISTS ").append(todayTableName).append(" (");
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        List<String> typeList = StringUtil.split(tableBean.getTbColTarMap().get(dsl_id), Constant.METAINFOSPLIT);
        for (int i = 0; i < columnList.size(); i++) {
            if (FileFormat.PARQUET.getCode().equals(file_format)) {
                String typeLower = typeList.get(i).toLowerCase();
                if (typeLower.contains(DataTypeConstant.DECIMAL.getMessage()) || typeLower.contains(DataTypeConstant.NUMERIC.getMessage()) || typeLower.contains(DataTypeConstant.DOUBLE.getMessage())) {
                    type = DataTypeConstant.DOUBLE.getMessage().toUpperCase();
                } else {
                    type = typeList.get(i);
                }
            } else {
                type = typeList.get(i);
            }
            sql.append("`").append(columnList.get(i)).append("` ").append(type).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") stored as ").append(hiveStored);
        return sql.toString();
    }

    public static String genHiveLoad(String todayTableName, long dsl_id, TableBean tableBean, String database_separatorr) {
        StringBuilder sql = new StringBuilder(120);
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        sql.append("CREATE TABLE IF NOT EXISTS ").append(todayTableName).append(" (");
        List<String> tarTypes = StringUtil.split(tableBean.getTbColTarMap().get(dsl_id), Constant.METAINFOSPLIT);
        List<String> types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        for (int i = 0; i < columnList.size(); i++) {
            if (!Constant.HYRENFIELD.contains(columnList.get(i).toUpperCase())) {
                if (!tarTypes.isEmpty() && StringUtil.isNotBlank(tarTypes.get(i)) && !"NULL".equalsIgnoreCase(tarTypes.get(i))) {
                    sql.append("`").append(columnList.get(i)).append("` ").append(tarTypes.get(i)).append(",");
                } else {
                    sql.append("`").append(columnList.get(i)).append("` ").append(types.get(i)).append(",");
                }
            } else {
                sql.append("`").append(columnList.get(i)).append("` ").append("string").append(",");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") ROW FORMAT SERDE 'org.apache.hadoop.hive.contrib.serde2.MultiDelimitSerDe' WITH  " + "SERDEPROPERTIES (\"field.delim\"=\"").append(database_separatorr).append("\") stored as  textfile");
        if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
            sql.append(" tblproperties (\"skip.header.line.count\"=\"1\")");
        }
        return sql.toString();
    }

    public static String genHiveDingChangLoad(String todayTableName, long dsl_id, TableBean tableBean, String database_separatorr) {
        StringBuilder sql = new StringBuilder(120);
        String file_code = DataBaseCode.ofValueByCode(tableBean.getFile_code());
        List<String> columnList = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        String columnsLengths = tableBean.getColLengthInfo().replace(Constant.METAINFOSPLIT, ",");
        sql.append("CREATE TABLE IF NOT EXISTS ").append(todayTableName).append(" (");
        List<String> tarTypes = StringUtil.split(tableBean.getTbColTarMap().get(dsl_id), Constant.METAINFOSPLIT);
        List<String> types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        for (int i = 0; i < columnList.size(); i++) {
            if (!Constant.HYRENFIELD.contains(columnList.get(i).toUpperCase())) {
                if (!tarTypes.isEmpty() && StringUtil.isNotBlank(tarTypes.get(i)) && !"NULL".equalsIgnoreCase(tarTypes.get(i))) {
                    sql.append("`").append(columnList.get(i)).append("` ").append(tarTypes.get(i)).append(",");
                } else {
                    sql.append("`").append(columnList.get(i)).append("` ").append(types.get(i)).append(",");
                }
            } else {
                sql.append("`").append(columnList.get(i)).append("` ").append("string").append(",");
            }
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") ROW FORMAT SERDE 'hrds.commons.hadoop.hive.serde.HyrenSerDe' WITH  " + "SERDEPROPERTIES (\"field.delim\"=\"").append(database_separatorr).append("\"," + "\"serialization.encoding\"=\"").append(file_code).append("\"," + "\"hyren.columns.lengths\"=\"").append(columnsLengths).append("\"" + ") stored as textfile");
        if (IsFlag.Shi.getCode().equals(tableBean.getIs_header())) {
            sql.append(" tblproperties (\"skip.header.line.count\"=\"1\")");
        }
        return sql.toString();
    }
}
