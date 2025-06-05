package hyren.serv6.g.interfaceindex;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.InterfaceUseLog;
import hyren.serv6.g.enumerate.StateType;
import org.springframework.stereotype.Service;

@Service
public class InterfaceIndexService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result interfaceResponseTime() {
        return Dbo.queryResult("select avg(response_time) avg,max(response_time) max,min(response_time) min," + "interface_name,interface_use_id " + " from " + InterfaceUseLog.TableName + " where request_state=? group by interface_name,interface_use_id", StateType.NORMAL.name());
    }
}
