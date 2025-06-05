package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.JobParamBean;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.constant.JobCollectTypeConstant;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;

@DocClass(desc = "", author = "WangZhengcheng")
public class JobFactory {

    private JobFactory() {
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "jobInfo", desc = "", range = "")
    @Param(name = "dbConfig", desc = "", range = "")
    @Param(name = "jobParam", desc = "", range = "")
    @Param(name = "statusFilePath", desc = "", range = "")
    @Param(name = "jobStatus", desc = "", range = "")
    @Return(desc = "", range = "")
    public static JobInterface newInstance(CollectTableBean collectTableBean, SourceDataConfBean sourceDataConfBean, JobParamBean jobParam, String statusFilePath, JobStatusInfo jobStatus) {
        JobInterface job;
        String collectType = jobParam.getCollect_type();
        if (JobCollectTypeConstant.DB_COLLECTION.equals(collectType)) {
            job = new DataBaseJobImpl(sourceDataConfBean, collectTableBean);
        } else {
            throw new IllegalArgumentException("还未支持的采集类型：" + collectType);
        }
        return job;
    }
}
