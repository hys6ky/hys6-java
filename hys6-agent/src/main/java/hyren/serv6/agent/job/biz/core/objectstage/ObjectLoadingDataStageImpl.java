package hyren.serv6.agent.job.biz.core.objectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dfstage.DFDataLoadingStageImpl;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.StoreLayerAdded;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.hadoop.i.IHbase;
import hyren.serv6.commons.hadoop.sqlutils.HSqlExecute;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.agent.constant.JobConstant;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/24 11:43")
public class ObjectLoadingDataStageImpl extends AbstractJobStage {

    private final ObjectTableBean objectTableBean;

    public ObjectLoadingDataStageImpl(ObjectTableBean objectTableBean) {
        this.objectTableBean = objectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集数据加载阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, objectTableBean.getOcs_id(), StageConstant.DATALOADING.getCode());
        try {
            String todayTableName = TableNameUtil.getUnderline1TableName(objectTableBean.getHyren_name());
            String hdfsPath = FileNameUtils.normalize(JobConstant.PREFIX + File.separator + objectTableBean.getOdc_id() + File.separator + objectTableBean.getHyren_name() + File.separator, true);
            List<DataStoreConfBean> dataStoreConfBeanList = objectTableBean.getDataStoreConfBean();
            for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type())) {
                    dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                    createHiveTableLoadData(todayTableName, hdfsPath, dataStoreConfBean, stageParamInfo.getTableBean());
                } else if (Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                    bulkloadLoadDataToHbase(todayTableName, hdfsPath, objectTableBean.getEtlDate(), dataStoreConfBean, stageParamInfo.getTableBean());
                }
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集数据加载阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), "执行失败");
            log.error("表" + objectTableBean.getEn_name() + "半结构化对象采集数据加载阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, objectTableBean, AgentType.DuiXiang.getCode());
        return stageParamInfo;
    }

    public static void createHiveTableLoadData(String todayTableName, String hdfsFilePath, DataStoreConfBean dataStoreConfBean, TableBean tableBean) {
        try (DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr())) {
            List<String> sqlList = new ArrayList<>();
            if (db.isExistTable(todayTableName)) {
                db.execute("DROP TABLE " + todayTableName);
            }
            sqlList.add(DFDataLoadingStageImpl.genHiveLoad(todayTableName, dataStoreConfBean.getDsl_id(), tableBean, tableBean.getColumn_separator()));
            sqlList.add("load data inpath '" + hdfsFilePath + "' into table " + todayTableName);
            HSqlExecute.executeSql(sqlList, db);
        } catch (Exception e) {
            throw new AppSystemException("执行hive加载数据的sql报错", e);
        }
    }

    @Override
    public int getStageCode() {
        return StageConstant.DATALOADING.getCode();
    }

    private void bulkloadLoadDataToHbase(String todayTableName, String hdfsFilePath, String etlDate, DataStoreConfBean dataStoreConfBean, TableBean tableBean) {
        String isMd5 = IsFlag.Fou.getCode();
        String columnMetaInfo = tableBean.getColumnMetaInfo();
        List<String> columnList = StringUtil.split(columnMetaInfo, Constant.METAINFOSPLIT);
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
        iHbase.loadObjDataToHBase(todayTableName, hdfsFilePath, tableBean, isMd5, rowKeyIndex, etlDate, dataStoreConfBean);
    }
}
