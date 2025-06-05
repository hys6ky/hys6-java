package hyren.serv6.g.usermanage;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.UserState;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.regular.RegexConstant;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.g.init.InterfaceManager;
import org.springframework.stereotype.Service;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Service
public class InterfaceUserManageService {

    @Method(desc = "", logicStep = "")
    @Param(name = "user_name", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public Result selectUserInfo(String user_name) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        assembler.clean();
        assembler.addSql("select user_name,user_id,user_password,user_email,user_remark from " + SysUser.TableName + " where create_id=?").addParam(UserUtil.getUserId());
        if (StringUtil.isNotBlank(user_name)) {
            assembler.addLikeParam("user_name", "%" + user_name + "%");
        }
        return Dbo.queryResult(assembler.sql(), assembler.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> selectUserById(long user_id) {
        return Dbo.queryOneObject("select user_id,user_name,user_email,user_remark,user_password from " + SysUser.TableName + " where user_id=?", user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sys_user", desc = "", range = "", isBean = true)
    public void addUser(SysUser sys_user) {
        checkFieldsForSysUser(sys_user.getUser_name(), sys_user.getUser_email(), sys_user.getUser_password());
        sys_user.setUser_id(Long.parseLong(PrimayKeyGener.getOperId()));
        sys_user.setUser_state(UserState.ZhengChang.getCode());
        sys_user.setCreate_date(DateUtil.getSysDate());
        sys_user.setCreate_time(DateUtil.getSysTime());
        sys_user.setUpdate_date(DateUtil.getSysDate());
        sys_user.setUpdate_time(DateUtil.getSysTime());
        sys_user.setRole_id(Long.parseLong("2001"));
        sys_user.setDep_id(UserUtil.getUser().getDepId());
        sys_user.setCreate_id(UserUtil.getUserId());
        sys_user.add(Dbo.db());
        InterfaceManager.initUser(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    public void deleteUser(long user_id) {
        DboExecute.deletesOrThrow("必须有一条数据被删除,", "delete from " + SysUser.TableName + " where user_id=?", user_id);
        Dbo.execute("delete from " + InterfaceUse.TableName + " where user_id = ?", user_id);
        Dbo.execute("delete from " + TableUseInfo.TableName + " where user_id = ?", user_id);
        Dbo.execute("delete from " + SysregParameterInfo.TableName + " where user_id = ?", user_id);
        InterfaceManager.initUser(Dbo.db());
        InterfaceManager.initInterface(Dbo.db());
        InterfaceManager.initTable(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_name", desc = "", range = "")
    @Param(name = "user_email", desc = "", range = "")
    @Param(name = "user_password", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "user_remark", desc = "", range = "", nullable = true)
    public void updateUser(String user_name, String user_email, String user_password, long user_id, String user_remark) {
        isUserExist(user_id);
        checkFieldsForSysUser(user_name, user_email, user_password);
        SysUser sys_user = new SysUser();
        sys_user.setUser_name(user_name);
        sys_user.setUser_email(user_email);
        sys_user.setUser_remark(user_remark);
        sys_user.setUser_password(user_password);
        sys_user.setUser_id(user_id);
        sys_user.setUpdate_time(DateUtil.getSysDate());
        sys_user.setUpdate_time(DateUtil.getSysTime());
        sys_user.update(Dbo.db());
        Result result = Dbo.queryResult("select * from " + InterfaceUse.TableName + " where user_id = ?", user_id);
        if (!result.isEmpty()) {
            Dbo.execute("update " + InterfaceUse.TableName + " set user_name = ? where user_id = ?", user_name, user_id);
        }
        Result logInfo = Dbo.queryResult("select * from " + InterfaceUseLog.TableName + " where user_id = ?", user_id);
        if (!logInfo.isEmpty()) {
            Dbo.execute("update " + InterfaceUseLog.TableName + " set user_name = ? where user_id = ?", user_name, user_id);
        }
        InterfaceManager.initUser(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    private void isUserExist(long user_id) {
        if (Dbo.queryNumber("select count(*) from " + SysUser.TableName + " where user_id=?", user_id).orElseThrow(() -> new BusinessException("sql查询错误")) != 1) {
            throw new BusinessException("当前登录用户对应用户信息已不存在，请检查");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sys_user", desc = "", range = "", isBean = true)
    private void checkFieldsForSysUser(String user_name, String user_email, String user_password) {
        if (StringUtil.isBlank(user_name)) {
            throw new BusinessException("用户名不能为空");
        }
        if (StringUtil.isBlank(user_password)) {
            throw new BusinessException("密码不能为空");
        }
        if (StringUtil.isBlank(user_email)) {
            throw new BusinessException("邮箱地址不能为空");
        }
        Pattern pattern = Pattern.compile(RegexConstant.EMAIL_VERIFICATION);
        Matcher matcher = pattern.matcher(user_email);
        if (!matcher.matches()) {
            throw new BusinessException("邮箱地址格式不正确");
        }
    }
}
