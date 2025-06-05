package hyren.serv6.agent.job.biz.core.jdbcdirectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dfstage.DFDataLoadingStageImpl;
import hyren.serv6.agent.job.biz.core.dfstage.DFUploadStageImpl;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class JdbcDirectDataLoadingStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    public JdbcDirectDataLoadingStageImpl(CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + collectTableBean.getTable_name() + "数据库直连采集加载阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.DATALOADING.getCode());
        try {
            String storageTableName = collectTableBean.getStorage_table_name();
            if (!JdbcDirectUnloadDataStageImpl.doAllSupportExternal(collectTableBean.getDataStoreConfBean())) {
                log.info("表" + storageTableName + "存储层不支持外部表，数据加载阶段不用做任何操作");
            } else {
                List<DataStoreConfBean> dataStoreConfBeanList = collectTableBean.getDataStoreConfBean();
                for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                    String todayTableName = TableNameUtil.getUnderline1TableName(storageTableName, collectTableBean.getStorage_type(), collectTableBean.getStorage_time());
                    String hdfsFilePath = DFUploadStageImpl.getUploadHdfsPath(collectTableBean);
                    if (Store_type.DATABASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        DFDataLoadingStageImpl.createExternalTableLoadData(todayTableName, collectTableBean, dataStoreConfBean, stageParamInfo.getTableBean(), stageParamInfo.getFileNameArr());
                    } else if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                        DFDataLoadingStageImpl.createHiveTableLoadData(todayTableName, hdfsFilePath, dataStoreConfBean, stageParamInfo.getTableBean(), collectTableBean);
                    } else if (Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        log.info("==========todayTableName:{}===========", todayTableName);
                        DFDataLoadingStageImpl.bulkloadLoadDataToHbase(todayTableName, hdfsFilePath, dataStoreConfBean, stageParamInfo.getTableBean(), collectTableBean);
                    } else if (Store_type.SOLR.getCode().equals(dataStoreConfBean.getStore_type())) {
                        log.warn("数据库直连采集数据加载进Solr没有实现");
                    } else if (Store_type.ElasticSearch.getCode().equals(dataStoreConfBean.getStore_type())) {
                        log.warn("数据库直连采集数据加载进ElasticSearch没有实现");
                    } else if (Store_type.MONGODB.getCode().equals(dataStoreConfBean.getStore_type())) {
                        log.warn("数据库直连采集数据加载进MONGODB没有实现");
                    } else {
                        throw new AppSystemException("表" + storageTableName + "不支持的存储类型");
                    }
                    log.info("数据成功进入库" + dataStoreConfBean.getDsl_name() + "下的表" + collectTableBean.getStorage_table_name());
                }
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + storageTableName + "数据库直连采集数据加载阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + collectTableBean.getStorage_table_name() + "数据库直连采集数据加载阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.ShuJuKu.getCode());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.DATALOADING.getCode();
    }
}
