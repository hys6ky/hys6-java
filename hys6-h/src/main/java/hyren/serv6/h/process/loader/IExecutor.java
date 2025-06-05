package hyren.serv6.h.process.loader;

import hyren.serv6.h.process.loader.executor.Executor;
import hyren.serv6.h.process.utils.ProcessJobRunStatusEnum;

public interface IExecutor {

    Executor registerBusiness(IBusiness iBusiness);

    Executor registerLoader(ILoader iLoader);

    Executor registerJobContext(IContext ijobContext);

    ProcessJobRunStatusEnum execute();
}
