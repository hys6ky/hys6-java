package hyren.serv6.agent.job.biz.core;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import hyren.serv6.agent.job.biz.bean.JobStatusInfo;
import hyren.serv6.agent.job.biz.bean.MetaInfoBean;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.jdbcdirectstage.*;
import hyren.serv6.agent.job.biz.utils.JobStatusInfoUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Slf4j
@DocClass(desc = "", author = "zxz")
public class JdbcDirectJobImpl implements JobInterface {

    private final CollectTableBean collectTableBean;

    private final SourceDataConfBean sourceDataConfBean;

    private final static Map<String, Thread> threadMap = new ConcurrentHashMap<>();

    public JdbcDirectJobImpl(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean) {
        this.sourceDataConfBean = sourceDataConfBean;
        this.collectTableBean = collectTableBean;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @Override
    public JobStatusInfo runJob() {
        String statusFilePath = Constant.JOBINFOPATH + sourceDataConfBean.getDatabase_id() + File.separator + collectTableBean.getTable_id() + File.separator + Constant.JOBFILENAME;
        JobStatusInfo jobStatusInfo = JobStatusInfoUtil.getStartJobStatusInfo(statusFilePath, collectTableBean.getTable_id(), collectTableBean.getTable_name());
        JobStageInterface unloadData = new JdbcDirectUnloadDataStageImpl(sourceDataConfBean, collectTableBean);
        JobStageInterface upload = new JdbcDirectUploadStageImpl(sourceDataConfBean, collectTableBean);
        JobStageInterface dataLoading = new JdbcDirectDataLoadingStageImpl(collectTableBean);
        JobStageInterface calIncrement = new JdbcDirectCalIncrementStageImpl(collectTableBean);
        JobStageInterface dataRegistration = new JdbcDirectDataRegistrationStageImpl(collectTableBean);
        JobStageController controller = new JobStageController();
        controller.registerJobStage(unloadData, upload, dataLoading, calIncrement, dataRegistration);
        try {
            if (collectTableBean.getInterval_time() != null && collectTableBean.getInterval_time() > 0) {
                try {
                    if (threadMap.get(collectTableBean.getTable_id()) == null) {
                        threadMap.put(collectTableBean.getTable_id(), Thread.currentThread());
                    } else {
                        threadMap.get(collectTableBean.getTable_id()).interrupt();
                    }
                    int interval_time = collectTableBean.getInterval_time();
                    do {
                        jobStatusInfo = controller.handleStageByOrder(statusFilePath, jobStatusInfo);
                        TimeUnit.SECONDS.sleep(interval_time);
                    } while (!DateUtil.getSysDate().equals(collectTableBean.getOver_date()));
                } catch (Exception e) {
                    throw new AppSystemException("数据库直连采集实时采集异常", e);
                }
            } else {
                jobStatusInfo = controller.handleStageByOrder(statusFilePath, jobStatusInfo);
            }
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
