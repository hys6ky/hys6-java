package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.StageParamInfo;

@DocClass(desc = "", author = "WangZhengcheng")
public interface JobStageInterface {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    StageParamInfo handleStage(StageParamInfo stageParamInfo) throws Exception;

    @Method(desc = "", logicStep = "")
    @Param(name = "stage", desc = "", range = "")
    void setNextStage(JobStageInterface stage);

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    JobStageInterface getNextStage();

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    int getStageCode();
}
