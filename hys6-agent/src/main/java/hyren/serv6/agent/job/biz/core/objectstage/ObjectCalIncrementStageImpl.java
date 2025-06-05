package hyren.serv6.agent.job.biz.core.objectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.core.dfstage.DFCalIncrementStageImpl;
import hyren.serv6.agent.job.biz.core.increasement.impl.IncreasementBySpark;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.codes.StorageType;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.codes.UpdateType;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.Increasement;
import hyren.serv6.commons.utils.agent.bean.DataStoreConfBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.storagelayer.StorageTypeKey;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class ObjectCalIncrementStageImpl extends AbstractJobStage {

    private final ObjectTableBean objectTableBean;

    public ObjectCalIncrementStageImpl(ObjectTableBean objectTableBean) {
        this.objectTableBean = objectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集计算增量阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, objectTableBean.getOcs_id(), StageConstant.CALINCREMENT.getCode());
        try {
            TableBean tableBean = stageParamInfo.getTableBean();
            List<DataStoreConfBean> dataStoreConfBeanList = objectTableBean.getDataStoreConfBean();
            for (DataStoreConfBean dataStoreConfBean : dataStoreConfBeanList) {
                Increasement increase = null;
                try {
                    if (Store_type.HIVE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        dataStoreConfBean.getData_store_connect_attr().put(StorageTypeKey.database_type, Store_type.HIVE.getValue());
                        DatabaseWrapper db = ConnectionTool.getDBWrapper(dataStoreConfBean.getData_store_connect_attr());
                        increase = new IncreasementBySpark(tableBean, objectTableBean.getHyren_name(), objectTableBean.getEtlDate(), db, dataStoreConfBean.getDsl_id());
                        if (UpdateType.DirectUpdate.getCode().equals(objectTableBean.getUpdatetype())) {
                            log.info("----------------------------直接更新--------------------------------");
                            increase.replace();
                        } else if (UpdateType.IncrementUpdate.getCode().equals(objectTableBean.getUpdatetype())) {
                            log.info("----------------------------拉链跟新--------------------------------");
                            increase.calculateIncrement();
                            increase.mergeIncrement();
                        }
                        increase.dropTodayTable();
                    } else if (Store_type.HBASE.getCode().equals(dataStoreConfBean.getStore_type())) {
                        increase = DFCalIncrementStageImpl.getHBaseIncreasement(tableBean, objectTableBean.getHyren_name(), objectTableBean.getEtlDate(), dataStoreConfBean);
                        if (UpdateType.DirectUpdate.getCode().equals(objectTableBean.getUpdatetype())) {
                            log.info("----------------------------直接更新--------------------------------");
                            increase.replace();
                        } else if (UpdateType.IncrementUpdate.getCode().equals(objectTableBean.getUpdatetype())) {
                            log.info("----------------------------拉链跟新--------------------------------");
                            increase.calculateIncrement();
                            increase.mergeIncrement();
                        }
                        increase.dropTodayTable();
                    }
                } catch (Exception e) {
                    if (increase != null) {
                        increase.restore(StorageType.TiHuan.getCatCode());
                    }
                    throw new AppSystemException("计算增量失败");
                } finally {
                    if (increase != null)
                        increase.close();
                }
            }
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + objectTableBean.getEn_name() + "半结构化对象采集计算增量阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), "执行失败");
            log.error("表" + objectTableBean.getEn_name() + "半结构化对象采集计算增量阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, objectTableBean, AgentType.DuiXiang.getCode());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.CALINCREMENT.getCode();
    }
}
