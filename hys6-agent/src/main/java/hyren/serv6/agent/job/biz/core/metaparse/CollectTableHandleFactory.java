package hyren.serv6.agent.job.biz.core.metaparse;

import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.agent.job.biz.core.metaparse.impl.DFCollectTableHandleParse;
import hyren.serv6.agent.job.biz.core.metaparse.impl.JdbcCollectTableHandleParse;
import hyren.serv6.agent.job.biz.core.metaparse.impl.JdbcDirectCollectTableHandleParse;
import hyren.serv6.base.codes.CollectType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.AppSystemException;

public class CollectTableHandleFactory {

    public static CollectTableHandle getCollectTableHandleInstance(SourceDataConfBean sourceDataConfBean) {
        if (IsFlag.Shi.getCode().equals(sourceDataConfBean.getDb_agent())) {
            return new DFCollectTableHandleParse();
        } else if (IsFlag.Fou.getCode().equals(sourceDataConfBean.getDb_agent())) {
            if (CollectType.ShuJuKuChouShu.getCode().equals(sourceDataConfBean.getCollect_type())) {
                return new JdbcCollectTableHandleParse();
            } else if (CollectType.ShuJuKuCaiJi.getCode().equals(sourceDataConfBean.getCollect_type())) {
                return new JdbcDirectCollectTableHandleParse();
            } else {
                throw new AppSystemException("数据库采集方式不正确");
            }
        } else {
            throw new AppSystemException("是否为db平面采集标识错误");
        }
    }
}
