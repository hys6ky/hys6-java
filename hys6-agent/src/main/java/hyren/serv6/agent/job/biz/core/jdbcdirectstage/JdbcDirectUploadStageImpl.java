package hyren.serv6.agent.job.biz.core.jdbcdirectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dfstage.DFDataLoadingStageImpl;
import hyren.serv6.agent.job.biz.core.dfstage.DFUploadStageImpl;
import hyren.serv6.agent.job.biz.core.jdbcdirectstage.service.CollectPage;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import hyren.serv6.commons.utils.xlstoxml.util.ConnUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class JdbcDirectUploadStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    private final SourceDataConfBean sourceDataConfBean;

    private final String operateDate;

    private final String operateTime;

    private final String user_id;

    public JdbcDirectUploadStageImpl(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
        this.operateDate = new SimpleDateFormat("yyyy-MM-dd").format(new Date());
        this.operateTime = new SimpleDateFormat("HH:mm:ss").format(new Date());
        this.user_id = String.valueOf(collectTableBean.getUser_id());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + collectTableBean.getTable_name() + "数据库直连采集上传阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.UPLOAD.getCode());
        try {
            long rowCount = 0;
            List<DataStoreConfBean> dataStoreConfBeanList = collectTableBean.getDataStoreConfBean();
            Store_type store_type;
            if (JdbcDirectUnloadDataStageImpl.doAllSupportExternal(collectTableBean.getDataStoreConfBean())) {
                for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                    store_type = Store_type.ofEnumByCode(dataStoreConfBean.getStore_type());
                    if (Store_type.DATABASE == store_type) {
                        DFUploadStageImpl.execSftpToDbServer(dataStoreConfBean, stageParamInfo.getFileArr(), collectTableBean);
                    } else if (Store_type.HIVE == store_type) {
                        dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                        DFUploadStageImpl.execHDFSShell(dataStoreConfBean, stageParamInfo.getFileArr(), collectTableBean);
                    } else if (Store_type.HBASE == store_type) {
                        DFUploadStageImpl.execHDFSShell(dataStoreConfBean, stageParamInfo.getFileArr(), collectTableBean);
                    } else if (Store_type.SOLR == store_type) {
                        throw new BusinessException("数据库直连采集数据上传进Solr, 不支持外部方式导入!");
                    } else if (Store_type.ElasticSearch == store_type) {
                        log.warn("数据库直连采集数据上传进ElasticSearch没有实现");
                    } else if (Store_type.MONGODB == store_type) {
                        log.warn("数据库直连采集数据上传进MONGODB没有实现");
                    } else {
                        throw new AppSystemException("不支持的存储类型");
                    }
                }
            } else {
                for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                    store_type = Store_type.ofEnumByCode(dataStoreConfBean.getStore_type());
                    if (Store_type.DATABASE == store_type) {
                        rowCount = jdbcToDataBase(stageParamInfo.getTableBean(), dataStoreConfBean);
                    } else if (Store_type.HIVE == store_type) {
                        dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                        rowCount = jdbcToDataBase(stageParamInfo.getTableBean(), dataStoreConfBean);
                    } else if (Store_type.HBASE == store_type) {
                        log.warn("数据库直连采集数据上传进HBASE没有实现: hbase类型的存储层" + dataStoreConfBean.getDsl_name() + "未选择支持外部表");
                    } else if (Store_type.SOLR == store_type) {
                        log.info("表 [ {} ] 数据库直连采集数据进Solr", collectTableBean.getTable_name());
                        rowCount = jdbcToSolr(stageParamInfo.getTableBean(), dataStoreConfBean);
                    } else if (Store_type.ElasticSearch == store_type) {
                        log.warn("数据库直连采集数据上传进ElasticSearch没有实现");
                    } else if (Store_type.MONGODB == store_type) {
                        log.warn("数据库直连采集数据上传进MONGODB没有实现");
                    } else {
                        throw new AppSystemException("不支持的存储类型");
                    }
                }
                stageParamInfo.setRowCount(rowCount);
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + collectTableBean.getTable_name() + "数据库直连采集上传阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + ",秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error(collectTableBean.getTable_name() + "数据库直连采集上传阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.ShuJuKu.getCode());
        return stageParamInfo;
    }

    private long jdbcToDataBase(TableBean tableBean, DataStoreConfBean dataStoreConfBean) {
        long rowCount = 0;
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        boolean flag = isSameJdbc(sourceDataConfBean.getJdbc_url(), sourceDataConfBean.getDatabase_type(), sourceDataConfBean.getDatabase_name(), data_store_connect_attr.get(StorageTypeKey.jdbc_url), data_store_connect_attr.get(StorageTypeKey.database_type), data_store_connect_attr.get(StorageTypeKey.database_name));
        Long storage_time = collectTableBean.getStorage_time();
        String todayTableName = TableNameUtil.getUnderline1TableName(collectTableBean.getStorage_table_name(), collectTableBean.getStorage_type(), storage_time);
        DatabaseWrapper db = null;
        try {
            db = ConnectionTool.getDBWrapper(data_store_connect_attr);
            if (storage_time > 0) {
                backupToDayTable(todayTableName, db);
                DFUploadStageImpl.createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
            } else {
                if (StorageType.TiHuan == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    dropToDayTable(todayTableName, db);
                    DFUploadStageImpl.createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
                } else if (StorageType.ZhuiJia == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    if (db.isExistTable(todayTableName)) {
                        if (db.getDbtype() == Dbtype.HIVE) {
                            DFDataLoadingStageImpl.backupTodayTableByAppend(todayTableName, tableBean, collectTableBean, db);
                        } else {
                            db.execute("DELETE FROM " + todayTableName + " WHERE " + Constant._HYREN_S_DATE + "='" + collectTableBean.getEtlDate() + "'");
                        }
                    } else {
                        DFUploadStageImpl.createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
                    }
                    db.commit();
                } else if (StorageType.ZengLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.QuanLiang == StorageType.ofEnumByCode(collectTableBean.getStorage_type()) || StorageType.LiShiLaLian == StorageType.ofEnumByCode(collectTableBean.getStorage_type())) {
                    backupToDayTable(todayTableName, db);
                    DFUploadStageImpl.createTodayTable(tableBean, todayTableName, dataStoreConfBean, db);
                }
            }
            log.info("======是否是同一JDBC: {} ======", flag);
            StorageType storageType = StorageType.ofEnumByCode(collectTableBean.getStorage_type());
            if (flag) {
                String insert_join = StringUtils.join(StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT), ",");
                String select_join;
                if (IsFlag.Shi == IsFlag.ofEnumByCode(collectTableBean.getIs_zipper())) {
                    String colMd5 = db.getDbtype().ofColMd5(db, StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT));
                    select_join = StringUtils.join(StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT), ",") + "," + collectTableBean.getEtlDate() + "," + collectTableBean.getValid_e_date() + "," + colMd5;
                } else if ((storageType == StorageType.TiHuan || storageType == StorageType.ZhuiJia) && IsFlag.ofEnumByCode(collectTableBean.getIs_md5()) == IsFlag.Shi) {
                    log.info("开始处理追加/替换方式中的MD5值");
                    String colMd5 = db.getDbtype().ofColMd5(db, StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT));
                    select_join = StringUtils.join(StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT), ",") + "," + collectTableBean.getEtlDate() + "," + colMd5;
                } else {
                    select_join = StringUtils.join(StringUtil.split(tableBean.getAllColumns(), Constant.METAINFOSPLIT), ",") + "," + collectTableBean.getEtlDate();
                }
                if (JobConstant.ISADDOPERATEINFO) {
                    select_join = select_join + ",'" + operateDate + "','" + operateTime + "'," + user_id;
                }
                if (tableBean.getCollectSQL().contains(Constant.SQLDELIMITER) || IsFlag.Shi.getCode().equals(collectTableBean.getIs_customize_sql())) {
                    List<String> parallelSqlList = StringUtil.split(tableBean.getCollectSQL(), Constant.SQLDELIMITER);
                    for (String sql : parallelSqlList) {
                        executeSameSql(db, sql, todayTableName, insert_join, select_join);
                    }
                } else {
                    executeSameSql(db, tableBean.getCollectSQL(), todayTableName, insert_join, select_join);
                }
            } else {
                List<Future<Long>> futures;
                if (tableBean.getCollectSQL().contains(Constant.SQLDELIMITER) || IsFlag.Shi.getCode().equals(collectTableBean.getIs_customize_sql())) {
                    futures = customizeParallelExtract(tableBean, dataStoreConfBean);
                } else {
                    futures = pageParallelExtract(tableBean, dataStoreConfBean);
                }
                for (Future<Long> future : futures) {
                    if (future.get() < 0) {
                        throw new AppSystemException("数据Batch提交到库" + dataStoreConfBean.getDsl_name() + "异常");
                    }
                    rowCount += future.get();
                }
            }
            backupPastTable(collectTableBean, db);
            log.info("数据成功进入库" + dataStoreConfBean.getDsl_name() + "下的表" + collectTableBean.getStorage_table_name() + ",总计进数" + rowCount + "条");
            return rowCount;
        } catch (Exception e) {
            if (db != null) {
                recoverBackupToDayTable(todayTableName, db);
            }
            throw new AppSystemException("数据库直连采集batch进库" + dataStoreConfBean.getDsl_name() + "下的表" + collectTableBean.getStorage_table_name() + "异常", e);
        } finally {
            if (db != null) {
                db.close();
            }
        }
    }

    private void executeSameSql(DatabaseWrapper db, String sql, String todayTableName, String insert_join, String select_join) {
        if (db.getDbtype() != Dbtype.POSTGRESQL) {
            List<String> tableNames = DruidParseQuerySql.parseSqlTableToList(sql);
            for (String table : tableNames) {
                if (!table.contains(".")) {
                    sql = StringUtil.replace(sql + " ", " " + table + " ", " " + sourceDataConfBean.getDatabase_name() + "." + table + " ");
                }
            }
        }
        log.info("======复制表数据的SQL: {} ======", sql);
        if (Dbtype.DB2V1 == db.getDbtype() || Dbtype.DB2V2 == db.getDbtype()) {
            db.execute("INSERT INTO " + todayTableName + "(" + insert_join + ")" + " ( SELECT " + select_join + " FROM ( " + sql + ") AS hyren_dcl_temp )");
        } else {
            db.execute("INSERT INTO " + todayTableName + "(" + insert_join + ")" + " SELECT " + select_join + " FROM ( " + sql + ") hyren_dcl_temp ");
        }
    }

    private boolean isSameJdbc(String source_jdbc_url, String source_database_type, String source_database_name, String target_jdbc_url, String target_database_type, String target_database_name) {
        Map<String, String> sourceJdbcUrlInfo = ConnUtil.getJDBCUrlInfo(source_jdbc_url, source_database_type);
        Map<String, String> targetJdbcUrlInfo = ConnUtil.getJDBCUrlInfo(target_jdbc_url, target_database_type);
        if (!source_database_type.equals(target_database_type)) {
            return false;
        } else {
            if (source_database_type.toLowerCase().contains("teradata")) {
                if (sourceJdbcUrlInfo.get("ip") == null || targetJdbcUrlInfo.get("ip") == null) {
                    return false;
                } else {
                    return sourceJdbcUrlInfo.get("ip").equals(targetJdbcUrlInfo.get("ip"));
                }
            } else if (source_database_type.toLowerCase().contains("postgresql")) {
                if (sourceJdbcUrlInfo.get("ip") == null || sourceJdbcUrlInfo.get("port") == null || targetJdbcUrlInfo.get("ip") == null || targetJdbcUrlInfo.get("port") == null || source_database_name == null || target_database_name == null) {
                    return false;
                } else {
                    return sourceJdbcUrlInfo.get("ip").equals(targetJdbcUrlInfo.get("ip")) && sourceJdbcUrlInfo.get("port").equals(targetJdbcUrlInfo.get("port")) && source_database_name.equals(target_database_name);
                }
            } else {
                if (sourceJdbcUrlInfo.get("ip") == null || sourceJdbcUrlInfo.get("port") == null || targetJdbcUrlInfo.get("ip") == null || targetJdbcUrlInfo.get("port") == null) {
                    return false;
                } else {
                    return sourceJdbcUrlInfo.get("ip").equals(targetJdbcUrlInfo.get("ip")) && sourceJdbcUrlInfo.get("port").equals(targetJdbcUrlInfo.get("port")) && source_database_name.equals(target_database_name);
                }
            }
        }
    }

    private List<Future<Long>> customizeParallelExtract(TableBean tableBean, DataStoreConfBean dataStoreConfBean) {
        ExecutorService executorService = null;
        try {
            List<Future<Long>> futures = new ArrayList<>();
            int lastPageEnd = Integer.MAX_VALUE;
            List<String> parallelSqlList = StringUtil.split(tableBean.getCollectSQL(), Constant.SQLDELIMITER);
            executorService = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
            for (String sql : parallelSqlList) {
                CollectPage lastPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, 1, lastPageEnd, dataStoreConfBean, sql);
                Future<Long> lastFuture = executorService.submit(lastPage);
                futures.add(lastFuture);
            }
            return futures;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            closeExecutor(executorService);
        }
    }

    private void closeExecutor(ExecutorService executorService) {
        if (executorService != null) {
            try {
                executorService.shutdown();
            } catch (Exception e) {
                log.warn("销毁线程池出现错误", e);
            }
        }
    }

    private List<Future<Long>> pageParallelExtract(TableBean tableBean, DataStoreConfBean dataStoreConfBean) {
        ExecutorService executorService = null;
        try {
            List<Future<Long>> futures = new ArrayList<>();
            int lastPageEnd = Integer.MAX_VALUE;
            if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_parallel())) {
                int totalCount = Integer.parseInt(collectTableBean.getTable_count());
                int days = DateUtil.dateMargin(collectTableBean.getRec_num_date(), collectTableBean.getEtlDate());
                days = Math.max(days, 0);
                totalCount += collectTableBean.getDataincrement() * days;
                int threadCount = collectTableBean.getPageparallels();
                if (threadCount > totalCount) {
                    throw new AppSystemException("多线程抽取数据,页面填写的多线程数大于表的总数据量");
                }
                int pageRow = totalCount / threadCount;
                executorService = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                for (int i = 0; i < threadCount; i++) {
                    int start = (i * pageRow) + 1;
                    int end = (i + 1) * pageRow;
                    CollectPage page = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, start, end, dataStoreConfBean);
                    Future<Long> future = executorService.submit(page);
                    futures.add(future);
                }
                int lastPageStart = pageRow * threadCount + 1;
                CollectPage lastPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, lastPageStart, lastPageEnd, dataStoreConfBean);
                Future<Long> lastFuture = executorService.submit(lastPage);
                futures.add(lastFuture);
            } else if (IsFlag.Fou.getCode().equals(collectTableBean.getIs_parallel())) {
                executorService = Executors.newFixedThreadPool(1);
                CollectPage collectPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, 1, lastPageEnd, dataStoreConfBean);
                Future<Long> lastFuture = executorService.submit(collectPage);
                futures.add(lastFuture);
            } else {
                throw new AppSystemException("错误的是否标识");
            }
            return futures;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            closeExecutor(executorService);
        }
    }

    private long jdbcToSolr(TableBean tableBean, DataStoreConfBean dataStoreConfBean) {
        long rowCount = 0;
        Map<String, String> data_store_connect_attr = dataStoreConfBean.getData_store_connect_attr();
        log.info("data_store_connect_attr: " + JsonUtil.toJson(data_store_connect_attr));
        try {
            List<Future<Long>> futures;
            IsFlag isCustomizeSqlFlag = IsFlag.ofEnumByCode(collectTableBean.getIs_customize_sql());
            if (tableBean.getCollectSQL().contains(Constant.SQLDELIMITER) || isCustomizeSqlFlag == IsFlag.Shi) {
                throw new BusinessException("自定义SQL采集并行抽取入Solr方式暂未实现!");
            } else {
                futures = pageParallelExtractToSolr(tableBean, dataStoreConfBean);
            }
            for (Future<Long> future : futures) {
                if (future.get() < 0) {
                    throw new AppSystemException("数据Batch提交到库" + dataStoreConfBean.getDsl_name() + "异常");
                }
                rowCount += future.get();
            }
        } catch (Exception e) {
            throw new AppSystemException("数据库直连采集batch进库" + dataStoreConfBean.getDsl_name() + "下的表" + collectTableBean.getStorage_table_name() + "异常", e);
        } finally {
        }
        return rowCount;
    }

    private List<Future<Long>> pageParallelExtractToSolr(TableBean tableBean, DataStoreConfBean dataStoreConfBean) {
        ExecutorService executorService = null;
        try {
            List<Future<Long>> futures = new ArrayList<>();
            int lastPageEnd = Integer.MAX_VALUE;
            if (IsFlag.Shi.getCode().equals(collectTableBean.getIs_parallel())) {
                int totalCount = Integer.parseInt(collectTableBean.getTable_count());
                int days = DateUtil.dateMargin(collectTableBean.getRec_num_date(), collectTableBean.getEtlDate());
                days = Math.max(days, 0);
                totalCount += collectTableBean.getDataincrement() * days;
                int threadCount = collectTableBean.getPageparallels();
                if (threadCount > totalCount) {
                    throw new AppSystemException("多线程抽取数据，页面填写的多线程数大于表的总数据量");
                }
                int pageRow = totalCount / threadCount;
                executorService = Executors.newFixedThreadPool(JobConstant.AVAILABLEPROCESSORS);
                for (int i = 0; i < threadCount; i++) {
                    int start = (i * pageRow) + 1;
                    int end = (i + 1) * pageRow;
                    CollectPage page = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, start, end, dataStoreConfBean);
                    Future<Long> future = executorService.submit(page);
                    futures.add(future);
                }
                int lastPageStart = pageRow * threadCount + 1;
                CollectPage lastPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, lastPageStart, lastPageEnd, dataStoreConfBean);
                Future<Long> lastFuture = executorService.submit(lastPage);
                futures.add(lastFuture);
            } else if (IsFlag.Fou.getCode().equals(collectTableBean.getIs_parallel())) {
                executorService = Executors.newFixedThreadPool(1);
                CollectPage collectPage = new CollectPage(sourceDataConfBean, collectTableBean, tableBean, 1, lastPageEnd, dataStoreConfBean);
                Future<Long> lastFuture = executorService.submit(collectPage);
                futures.add(lastFuture);
            } else {
                throw new AppSystemException("错误的是否标识");
            }
            return futures;
        } catch (Exception e) {
            throw new AppSystemException("执行分页卸数程序失败", e);
        } finally {
            closeExecutor(executorService);
        }
    }

    @Override
    public int getStageCode() {
        return StageConstant.UPLOAD.getCode();
    }
}
