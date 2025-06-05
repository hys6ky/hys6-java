package hyren.serv6.agent.job.biz.core.dfstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.FileUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.utils.CommunicationUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.entity.DataStoreReg;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DFDataRegistrationStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    public DFDataRegistrationStageImpl(CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        String storageTableName = collectTableBean.getStorage_table_name();
        log.info("------------------表" + storageTableName + "DB文件采集数据登记阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.DATAREGISTRATION.getCode());
        try {
            DataStoreReg data_store_reg = new DataStoreReg();
            data_store_reg.setAgent_id(collectTableBean.getAgent_id());
            data_store_reg.setDatabase_id(Long.parseLong(collectTableBean.getDatabase_id()));
            data_store_reg.setTable_id(Long.parseLong(collectTableBean.getTable_id()));
            data_store_reg.setSource_id(collectTableBean.getSource_id());
            data_store_reg.setCollect_type(AgentType.DBWenJian.getCode());
            data_store_reg.setFile_size(stageParamInfo.getFileSize());
            data_store_reg.setHyren_name(storageTableName);
            data_store_reg.setTable_name(collectTableBean.getTable_name());
            data_store_reg.setOriginal_name(collectTableBean.getTable_ch_name());
            data_store_reg.setOriginal_update_date(DateUtil.getSysDate());
            data_store_reg.setOriginal_update_time(DateUtil.getSysTime());
            data_store_reg.setStorage_date(DateUtil.getSysDate());
            data_store_reg.setStorage_time(DateUtil.getSysTime());
            Map<String, Object> metaInfoObj = new HashMap<>();
            metaInfoObj.put("records", stageParamInfo.getRowCount());
            metaInfoObj.put("mr", "n");
            TableBean tableBean = stageParamInfo.getTableBean();
            metaInfoObj.put("column", tableBean.getColumnMetaInfo());
            metaInfoObj.put("length", tableBean.getColLengthInfo());
            metaInfoObj.put("fileSize", stageParamInfo.getFileSize());
            metaInfoObj.put("tableName", collectTableBean.getTable_name());
            metaInfoObj.put("type", tableBean.getColTypeMetaInfo());
            data_store_reg.setMeta_info(JsonUtil.toJson(metaInfoObj));
            CommunicationUtil.addDataStoreReg(data_store_reg, collectTableBean.getDatabase_id());
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            String unloadFileAbsolutePath = FileNameUtils.normalize(Constant.DBFILEUNLOADFOLDER + collectTableBean.getDatabase_id() + File.separator + storageTableName + File.separator + collectTableBean.getEtlDate() + File.separator, true);
            File dir = new File(unloadFileAbsolutePath);
            if (dir.exists()) {
                FileUtil.cleanDirectory(dir);
            }
            log.info("------------------表" + storageTableName + "DB文件采集数据登记阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + storageTableName + "DB文件采集数据登记阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.DBWenJian.getCode());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.DATAREGISTRATION.getCode();
    }
}
