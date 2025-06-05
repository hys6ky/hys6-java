package hyren.serv6.agent.job.biz.core.objectstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.utils.CommunicationUtil;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.base.entity.DataStoreReg;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import lombok.extern.slf4j.Slf4j;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/24 14:27")
public class ObjectRegistrationStageImpl extends AbstractJobStage {

    private final ObjectTableBean objectTableBean;

    public ObjectRegistrationStageImpl(ObjectTableBean objectTableBean) {
        this.objectTableBean = objectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + objectTableBean.getEn_name() + "数据库抽数数据登记阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, objectTableBean.getOcs_id(), StageConstant.DATAREGISTRATION.getCode());
        try {
            DataStoreReg data_store_reg = new DataStoreReg();
            data_store_reg.setAgent_id(objectTableBean.getAgent_id());
            data_store_reg.setDatabase_id(Long.parseLong(objectTableBean.getOdc_id()));
            data_store_reg.setTable_id(Long.parseLong(objectTableBean.getOcs_id()));
            data_store_reg.setSource_id(objectTableBean.getSource_id());
            data_store_reg.setCollect_type(AgentType.DuiXiang.getCode());
            data_store_reg.setFile_size(stageParamInfo.getFileSize());
            data_store_reg.setHyren_name(objectTableBean.getHyren_name());
            data_store_reg.setTable_name(objectTableBean.getEn_name());
            data_store_reg.setOriginal_name(objectTableBean.getZh_name());
            data_store_reg.setOriginal_update_date(DateUtil.getSysDate());
            data_store_reg.setOriginal_update_time(DateUtil.getSysTime());
            data_store_reg.setStorage_date(DateUtil.getSysDate());
            data_store_reg.setStorage_time(DateUtil.getSysTime());
            Map<String, Object> metaInfoObj = new HashMap<>();
            TableBean tableBean = stageParamInfo.getTableBean();
            metaInfoObj.put("column", tableBean.getColumnMetaInfo());
            metaInfoObj.put("length", tableBean.getColLengthInfo());
            metaInfoObj.put("fileSize", stageParamInfo.getFileSize());
            metaInfoObj.put("tableName", objectTableBean.getEn_name());
            metaInfoObj.put("type", tableBean.getColTypeMetaInfo());
            data_store_reg.setMeta_info(JsonUtil.toJson(metaInfoObj));
            CommunicationUtil.addDataStoreReg(data_store_reg, objectTableBean.getOdc_id());
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + objectTableBean.getEn_name() + "数据库抽数数据登记阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + objectTableBean.getEn_name() + "数据库抽数数据登记阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, objectTableBean, AgentType.DuiXiang.getCode());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.DATAREGISTRATION.getCode();
    }
}
