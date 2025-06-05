package hyren.serv6.agent.job.biz.core.metaparse;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.agent.job.biz.bean.SourceDataConfBean;
import hyren.serv6.commons.utils.agent.bean.CollectTableBean;
import hyren.serv6.commons.utils.agent.bean.TableBean;

@DocClass(desc = "", author = "zxz", createdate = "2020/3/26 17:16")
public interface CollectTableHandle {

    @Method(desc = "", logicStep = "")
    @Param(name = "sourceDataConfBean", desc = "", range = "")
    @Param(name = "collectTableBean", desc = "", range = "")
    @Return(desc = "", range = "")
    TableBean generateTableInfo(SourceDataConfBean sourceDataConfBean, CollectTableBean collectTableBean);
}
