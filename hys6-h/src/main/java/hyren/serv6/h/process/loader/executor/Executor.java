package hyren.serv6.h.process.loader.executor;

import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.h.process.loader.IBusiness;
import hyren.serv6.h.process.loader.IContext;
import hyren.serv6.h.process.loader.IExecutor;
import hyren.serv6.h.process.loader.ILoader;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Executor implements IExecutor {

    private ILoader iLoader;

    private IBusiness iBusiness;

    private IContext iContext;

    @Override
    public Executor registerBusiness(IBusiness iBusiness) {
        this.iBusiness = iBusiness;
        return this;
    }

    @Override
    public Executor registerLoader(ILoader iLoader) {
        this.iLoader = iLoader;
        return this;
    }

    @Override
    public Executor registerJobContext(IContext iJobContext) {
        this.iContext = iJobContext;
        return this;
    }

    @Override
    public ProcessJobRunStatusEnum execute() {
        boolean is_success = false;
        ProcessJobRunStatusEnum jobRunStatus;
        try {
            iContext.startJob();
            IsFlag isTempFlag = iLoader.getProcessJobTableConfBean().getIsTempFlag();
            if (isTempFlag == IsFlag.Shi) {
                iBusiness.handleTempTableJob();
            } else {
                iBusiness.handleModuleTableJob();
            }
            jobRunStatus = iLoader.getJobRunStatus();
            log.info("iLoader.getJobRunStatus().getCode(): {}", jobRunStatus.getCode());
            is_success = jobRunStatus == ProcessJobRunStatusEnum.FINISHED;
        } finally {
            iContext.endJob(is_success);
        }
        return jobRunStatus;
    }
}
