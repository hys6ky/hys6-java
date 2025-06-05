package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;

@DocClass(desc = "", author = "WangZhengcheng")
public interface JobInterface extends MetaInfoInterface {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    JobStatusInfo runJob();
}
