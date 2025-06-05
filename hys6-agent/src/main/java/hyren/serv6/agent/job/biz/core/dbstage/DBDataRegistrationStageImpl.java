package hyren.serv6.agent.job.biz.core.dbstage;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;
import hyren.serv6.agent.job.biz.bean.StageStatusInfo;
import hyren.serv6.agent.job.biz.constant.RunStatusConstant;
import hyren.serv6.agent.job.biz.core.AbstractJobStage;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.codes.AgentType;
import hyren.serv6.commons.enumtools.StageConstant;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DBDataRegistrationStageImpl extends AbstractJobStage {

    private final CollectTableBean collectTableBean;

    public DBDataRegistrationStageImpl(CollectTableBean collectTableBean) {
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public StageParamInfo handleStage(StageParamInfo stageParamInfo) {
        long startTime = System.currentTimeMillis();
        log.info("------------------表" + collectTableBean.getTable_name() + "数据库抽数数据登记阶段开始------------------");
        StageStatusInfo statusInfo = new StageStatusInfo();
        JobStatusInfoUtil.startStageStatusInfo(statusInfo, collectTableBean.getTable_id(), StageConstant.DATAREGISTRATION.getCode());
        try {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.SUCCEED.getCode(), "执行成功");
            log.info("------------------表" + collectTableBean.getTable_name() + "数据库抽数数据登记阶段成功------------------执行时间为：" + (System.currentTimeMillis() - startTime) / 1000 + "，秒");
        } catch (Exception e) {
            JobStatusInfoUtil.endStageStatusInfo(statusInfo, RunStatusConstant.FAILED.getCode(), e.getMessage());
            log.error("表" + collectTableBean.getTable_name() + "数据库抽数数据登记阶段失败：", e);
        }
        JobStatusInfoUtil.endStageParamInfo(stageParamInfo, statusInfo, collectTableBean, AgentType.ShuJuKu.getCode());
        return stageParamInfo;
    }

    @Override
    public int getStageCode() {
        return StageConstant.DATAREGISTRATION.getCode();
    }
}
