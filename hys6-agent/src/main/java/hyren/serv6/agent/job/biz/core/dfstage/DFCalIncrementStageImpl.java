package hyren.serv6.agent.job.biz.core.dfstage;

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
import hyren.serv6.agent.job.biz.core.databaseadditinfo.DatabaseAdditInfoOperateInterface;
import hyren.serv6.agent.job.biz.core.databaseadditinfo.impl.DB2AdditInfoOperateImpl;
import hyren.serv6.agent.job.biz.core.databaseadditinfo.impl.OracleAdditInfoOperateImpl;
import hyren.serv6.agent.job.biz.core.databaseadditinfo.impl.PostgresqlAdditInfoOperateImpl;
import hyren.serv6.agent.job.biz.core.increasement.JDBCIncreasement;
import hyren.serv6.agent.job.biz.core.increasement.impl.IncreasementByMpp;
import hyren.serv6.agent.job.biz.core.increasement.impl.IncreasementBySpark;
import hyren.serv6.commons.utils.ColUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.Increasement;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.CollectTableColumnBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DFCalIncrementStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    public DFCalIncrementStageImpl(CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        String storageTableName = collectTableBean.getStorage_table_name();
        log.info("------------------表" + storageTableName + "DB文件采集增量计算阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.CALINCREMENT.getCode());
        try {
            if (UnloadType.ZengLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                log.info("表" + storageTableName + "增量卸数计算增量阶段不用做任何操作");
            } else if (UnloadType.QuanLiangXieShu.getCode().equals(collectTableBean.getUnload_type())) {
                List<DataStoreConfBean> dataStoreConfBeanList = collectTableBean.getDataStoreConfBean();
                TableBean tableBean = stageParamInfo.getTableBean();
                for (DataStoreConfBean dataStoreConf : dataStoreConfBeanList) {
                    if (Store_type.DATABASE.getCode().equals(dataStoreConf.getStore_type()) || Store_type.HIVE.getCode().equals(dataStoreConf.getStore_type())) {
                        JDBCIncreasement increase = null;
                        try {
                            if (Store_type.HIVE.getCode().equals(dataStoreConf.getStore_type())) {
                                dataStoreConf.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                            }
                            DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConf.getData_store_connect_attr());
                            increase = getJdbcIncreasement(tableBean, storageTableName, collectTableBean.getEtlDate(), db, dataStoreConf.getDsl_id());
                            execIncreasement(increase);
                            configureAdditInfo(storageTableName, dataStoreConf, db);
                            columnComment(db, dataStoreConf.getDsl_id(), tableBean);
                            db.commit();
                        } catch (Exception e) {
                            if (increase != null) {
                                increase.restore(collectTableBean.getStorage_type());
                            }
                            throw new AppSystemException("计算增量失败");
                        } finally {
                            if (increase != null) {
                                increase.close();
                            }
                        }
                    } else if (Store_type.HBASE.getCode().equals(dataStoreConf.getStore_type())) {
                        Increasement increase = null;
                        try {
                            increase = getHBaseIncreasement(tableBean, storageTableName, collectTableBean.getEtlDate(), dataStoreConf);
                            execIncreasement(increase);
                        } catch (Exception e) {
                            if (increase != null) {
                                increase.restore(collectTableBean.getStorage_type());
                            }
                            throw new AppSystemException("计算增量失败", e);
                        } finally {
                            if (increase != null) {
                                increase.close();
                            }
                        }
                    } else if (Store_type.SOLR.getCode().equals(dataStoreConf.getStore_type())) {
                        log.info("数据进Solr是以替换方式进数,计算增量阶段不做任何操作 ....");
                    } else if (Store_type.ElasticSearch.getCode().equals(dataStoreConf.getStore_type())) {
                        log.warn("数据进ElasticSearch待实现");
                    } else if (Store_type.MONGODB.getCode().equals(dataStoreConf.getStore_type())) {
                        log.warn("数据进MONGODB待实现");
                    } else {
                        throw new AppSystemException("表" + storageTableName + "不支持的存储类型");
                    }
                }
            } else {
                throw new AppSystemException("表" + storageTableName + "DB文件采集指定的数据抽取卸数方式类型不正确");
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + storageTableName + "DB文件采集增量阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + storageTableName + "DB文件采集增量阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.DBWenJian.getCode());
        return stageParamInfo;
    }

    public static Increasement getHBaseIncreasement(TableBean tableBean, String hbase_name, String etlDate, DataStoreConfBean dataStoreConf) {
        return ClassBase.HbaseInstance().getHBaseIncreasement(tableBean, hbase_name, etlDate, dataStoreConf);
    }

    private void configureAdditInfo(String hbase_name, DataStoreConfBean dataStoreConfBean, DatabaseWrapper db) {
        Map<String, Map<String, Integer>> additInfoFieldMap = dataStoreConfBean.getAdditInfoFieldMap();
        String database_type = dataStoreConfBean.getData_store_connect_attr().get(StorageTypeKey.database_type);
        if (additInfoFieldMap != null && !additInfoFieldMap.isEmpty()) {
            DatabaseAdditInfoOperateInterface additInfoOperateInterface;
            Dbtype dbType = ConnectionTool.getDbType(database_type);
            if (dbType == Dbtype.POSTGRESQL) {
                additInfoOperateInterface = new PostgresqlAdditInfoOperateImpl();
            } else if (dbType == Dbtype.ORACLE) {
                additInfoOperateInterface = new OracleAdditInfoOperateImpl();
            } else if (dbType == Dbtype.DB2V1 || dbType == Dbtype.DB2V2) {
                additInfoOperateInterface = new DB2AdditInfoOperateImpl();
            } else {
                log.warn("数据文件采集暂时还实现数据库配置主键和索引的功能" + dbType);
                return;
            }
            for (String dsla_storelayer : additInfoFieldMap.keySet()) {
                Map<String, Integer> map = additInfoFieldMap.get(dsla_storelayer);
                List<String> columnList = new ArrayList<>();
                map.forEach((key, value) -> columnList.add(key));
                StoreLayerAdded sla_Enum = StoreLayerAdded.ofEnumByCode(dsla_storelayer);
                if (sla_Enum == StoreLayerAdded.ZhuJian) {
                    additInfoOperateInterface.addPkConstraint(hbase_name, columnList, db);
                } else if (sla_Enum == StoreLayerAdded.SuoYinLie) {
                    additInfoOperateInterface.addNormalIndex(hbase_name, columnList, db);
                } else {
                    throw new AppSystemException("数据库" + dbType + " ,不支持" + sla_Enum.getValue() + "操作");
                }
            }
        }
    }

    private JDBCIncreasement getJdbcIncreasement(TableBean tableBean, String hbase_name, String etlDate, DatabaseWrapper db, long dsl_id) {
        if (Dbtype.HIVE.equals(db.getDbtype())) {
            return new IncreasementBySpark(tableBean, hbase_name, etlDate, db, dsl_id);
        } else {
            return new IncreasementByMpp(tableBean, hbase_name, etlDate, db, dsl_id);
        }
    }

    private void execIncreasement(Increasement increasement) {
        if (StorageType.QuanLiang.getCode().equals(collectTableBean.getStorage_type())) {
            log.info("----------------------------F5:全历史拉链--------------------------------");
            increasement.calculateIncrement();
            increasement.mergeIncrement();
        } else if (StorageType.LiShiLaLian.getCode().equals(collectTableBean.getStorage_type())) {
            log.info("----------------------------F3:历史拉链--------------------------------");
            increasement.incrementalDataZipper();
        } else if (StorageType.ZengLiang.getCode().equals(collectTableBean.getStorage_type())) {
            log.info("----------------------------全量卸数不支持增量拉链--------------------------------");
        } else if (StorageType.ZhuiJia.getCode().equals(collectTableBean.getStorage_type())) {
            log.info("----------------------------I:追加--------------------------------");
            increasement.append();
        } else if (StorageType.TiHuan.getCode().equals(collectTableBean.getStorage_type())) {
            log.info("----------------------------F1:替换--------------------------------");
            increasement.replace();
        } else {
            throw new AppSystemException("表" + collectTableBean.getStorage_table_name() + "请选择正确的存储方式！");
        }
    }

    @Override
    public int getStageCode() {
        return StageConstant.CALINCREMENT.getCode();
    }

    void columnComment(DatabaseWrapper db, long dsl_id, TableBean tableBean) {
        log.info("开始执行数据表: " + collectTableBean.getStorage_table_name() + " 的列注释修改");
        Map<String, String> columns = collectTableBean.getCollectTableColumnBeanList().stream().filter(column -> !Constant.HYRENFIELD.contains(column.getColumn_name().toUpperCase())).collect(Collectors.toMap(CollectTableColumnBean::getColumn_name, CollectTableColumnBean::getColumn_ch_name));
        collectTableBean.setDb_type(db.getDbtype());
        Map<String, String> columnType = getColumnType(dsl_id, tableBean);
        log.info("------------------表" + collectTableBean.getTable_name() + "开始修改字段长度信息------------------" + columnType);
        collectTableBean.getDb_type().databaseComment(db, collectTableBean.getStorage_table_name(),
                collectTableBean.getTable_ch_name(),columns, columnType);
    }

    Map<String, String> getColumnType(long dsl_id, TableBean tableBean) {
        List<String> types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        List<String> columns = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        List<String> tar_types = ColUtil.getTarTypes(tableBean, dsl_id);
        Map<String, String> columnType = new HashMap<>();
        for (int i = 0; i < columns.size(); i++) {
            if (!Constant.HYRENFIELD.contains(columns.get(i).toUpperCase())) {
                if (!tar_types.isEmpty() && StringUtil.isNotBlank(tar_types.get(i)) && !tar_types.get(i).equalsIgnoreCase("NULL")) {
                    columnType.put(columns.get(i).toLowerCase(), tar_types.get(i));
                } else {
                    columnType.put(columns.get(i).toLowerCase(), types.get(i));
                }
            }
        }
        return columnType;
    }
}
