package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.MetaInfoBean;
import java.util.List;
import java.util.concurrent.Callable;

@DocClass(desc = "", author = "WangZhengcheng")
public interface MetaInfoInterface extends Callable<JobStatusInfo> {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    List<MetaInfoBean> getMetaInfoGroup();

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    MetaInfoBean getMetaInfo();
}
