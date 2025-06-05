package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.MetaInfoBean;
import hyren.serv6.agent.job.biz.bean.ObjectCollectParamBean;
import hyren.serv6.agent.job.biz.bean.ObjectTableBean;
import hyren.serv6.agent.job.biz.core.objectstage.*;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2019/10/23 17:50")
public class ObjectCollectJobImpl implements JobInterface {

    private final ObjectCollectParamBean objectCollectParamBean;

    private final ObjectTableBean objectTableBean;

    public ObjectCollectJobImpl(ObjectCollectParamBean objectCollectParamBean, ObjectTableBean objectTableBean) {
        this.objectCollectParamBean = objectCollectParamBean;
        this.objectTableBean = objectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public JobStatusInfo runJob() {
        String statusFilePath = Constant.JOBINFOPATH + objectCollectParamBean.getOdc_id() + File.separator + objectTableBean.getOcs_id() + File.separator + Constant.JOBFILENAME;
        JobStatusInfo jobStatus = JobStatusInfoUtil.getStartJobStatusInfo(statusFilePath, objectTableBean.getOcs_id(), objectTableBean.getEn_name());
        JobStageInterface unloadData = new ObjectUnloadDataStageImpl(objectCollectParamBean, objectTableBean);
        JobStageInterface upload = new ObjectUploadStageImpl(objectTableBean);
        JobStageInterface dataLoading = new ObjectLoadingDataStageImpl(objectTableBean);
        JobStageInterface calIncrement = new ObjectCalIncrementStageImpl(objectTableBean);
        JobStageInterface dataRegistration = new ObjectRegistrationStageImpl(objectTableBean);
        JobStageController controller = new JobStageController();
        controller.registerJobStage(unloadData, upload, dataLoading, calIncrement, dataRegistration);
        try {
            jobStatus = controller.handleStageByOrder(statusFilePath, jobStatus);
        } catch (Exception e) {
            log.error("对象采集异常", e);
        }
        return jobStatus;
    }

    @Override
    public List<MetaInfoBean> getMetaInfoGroup() {
        return null;
    }

    @Override
    public MetaInfoBean getMetaInfo() {
        return null;
    }

    @Override
    public JobStatusInfo call() {
        return runJob();
    }
}
