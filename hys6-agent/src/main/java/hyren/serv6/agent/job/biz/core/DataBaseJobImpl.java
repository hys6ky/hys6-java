package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.MetaInfoBean;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.dbstage.*;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "WangZhengcheng")
public class DataBaseJobImpl implements JobInterface {

    private final CollectTableBean collectTableBean;

    private final SourceDataConfBean sourceDataConfBean;

    public DataBaseJobImpl(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public JobStatusInfo runJob() {
        String statusFilePath = Constant.JOBINFOPATH + sourceDataConfBean.getDatabase_id() + File.separator + collectTableBean.getTable_id() + File.separator + Constant.fileFormatMap.get(collectTableBean.getSelectFileFormat()) + File.separator + Constant.JOBFILENAME;
        JobStatusInfo jobStatusInfo = JobStatusInfoUtil.getStartJobStatusInfo(statusFilePath, collectTableBean.getTable_id(), collectTableBean.getTable_name());
        JobStageInterface unloadData = new DBUnloadDataStageImpl(sourceDataConfBean, collectTableBean);
        JobStageInterface upload = new DBUploadStageImpl(collectTableBean);
        JobStageInterface dataLoading = new DBDataLoadingStageImpl(collectTableBean);
        JobStageInterface calIncrement = new DBCalIncrementStageImpl(collectTableBean);
        JobStageInterface dataRegistration = new DBDataRegistrationStageImpl(collectTableBean);
        JobStageController controller = new JobStageController();
        controller.registerJobStage(unloadData, upload, dataLoading, calIncrement, dataRegistration);
        try {
            jobStatusInfo = controller.handleStageByOrder(statusFilePath, jobStatusInfo);
        } catch (Exception e) {
            log.error("数据库采集异常", e);
        }
        return jobStatusInfo;
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
