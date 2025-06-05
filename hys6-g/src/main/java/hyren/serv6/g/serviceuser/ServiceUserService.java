package hyren.serv6.g.serviceuser;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.InterfaceUse;
import hyren.serv6.base.entity.SysregParameterInfo;
import hyren.serv6.base.entity.TableUseInfo;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.RequestUtil;
import hyren.serv6.commons.utils.constant.PropertyParaValue;
import org.springframework.stereotype.Service;

@Service
public class ServiceUserService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sysreg_name", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Result searchDataTableInfo(String sysreg_name) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("SELECT sysreg_name,original_name,use_id FROM " + TableUseInfo.TableName + " WHERE user_id = ?").addParam(UserUtil.getUserId());
        if (StringUtil.isNotBlank(sysreg_name)) {
            assembler.addSql(" and sysreg_name like ?").addParam("%" + sysreg_name + "%");
        }
        return Dbo.queryResult(assembler.sql(), assembler.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "interface_name", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Result searchInterfaceInfo(String interface_name) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("SELECT * FROM " + InterfaceUse.TableName + " WHERE user_id = ?").addParam(UserUtil.getUserId());
        if (StringUtil.isNotBlank(interface_name)) {
            assembler.addLikeParam("interface_name", "%" + interface_name + "%");
        }
        assembler.addSql(" order by interface_use_id");
        return Dbo.queryResult(assembler.sql(), assembler.params());
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public String getIpAndPort() {
        return PropertyParaValue.getString("hyren_host", "127.0.0.1") + ":" + RequestUtil.getRequest().getLocalPort();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "use_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result searchColumnInfoById(long use_id) {
        return Dbo.queryResult("SELECT parameter_id,table_ch_column,table_en_column FROM " + SysregParameterInfo.TableName + " WHERE use_id = ? and user_id=?", use_id, UserUtil.getUserId());
    }
}
