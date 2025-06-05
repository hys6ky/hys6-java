package hyren.serv6.a.sysuser;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.a.department.DepartmentService;
import hyren.serv6.base.codes.UserState;
import hyren.serv6.base.entity.DepartmentInfo;
import hyren.serv6.base.entity.SysUser;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
@Service
@DocClass(desc = "", author = "BY-HLL", createdate = "2019/10/17 0017")
public class SysUserService {

    @Method(desc = "", logicStep = "")
    @Param(name = "sysUser", desc = "", range = "", example = "", isBean = true)
    @Return(desc = "", range = "")
    public void saveSysUser(SysUser sysUser) {
        if (StringUtil.isBlank(sysUser.getDep_id().toString())) {
            throw new BusinessException("部门不能为空，dep_id=" + sysUser.getDep_id());
        }
        DepartmentService dep = new DepartmentService();
        if (!dep.checkDepIdIsExist(sysUser.getDep_id())) {
            throw new BusinessException("部门不存在，dep_id=" + sysUser.getDep_id());
        }
        sysUser.setUser_id(Long.parseLong(PrimayKeyGener.getOperId()));
        sysUser.setCreate_id(UserUtil.getUserId());
        sysUser.setUser_state(UserState.ZhengChang.getCode());
        sysUser.setCreate_date(DateUtil.getSysDate());
        sysUser.setCreate_time(DateUtil.getSysTime());
        sysUser.setUpdate_date(DateUtil.getSysDate());
        sysUser.setUpdate_time(DateUtil.getSysTime());
        sysUser.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sys_user", desc = "", range = "", example = "", isBean = true)
    @Return(desc = "", range = "")
    public void updateSysUser(SysUser sys_user) {
        if (checkSysUserIsNotExist(sys_user.getUser_id())) {
            throw new BusinessException("修改不存在的用户id，user_id=" + sys_user.getUser_id());
        }
        sys_user.setUpdate_date(DateUtil.getSysDate());
        sys_user.setUpdate_time(DateUtil.getSysTime());
        sys_user.update(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "", nullable = true)
    @Return(desc = "", range = "")
    public SysUser getSysUserByUserId(long user_id) {
        if (checkSysUserIsNotExist(user_id)) {
            throw new BusinessException("查询不存在的用户id，user_id=" + user_id);
        }
        return Dbo.queryOneObject(SysUser.class, "select * from " + SysUser.TableName + " where user_id = ?", user_id).orElseThrow(() -> new BusinessException("根据用户id获取用户信息的SQL失败!"));
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    public void deleteSysUser(long user_id) {
        if (checkSysUserIsNotExist(user_id)) {
            throw new BusinessException("删除不存在的用户id，user_id=" + user_id);
        }
        DboExecute.deletesOrThrow("删除系统用户失败!，user_id=" + user_id, "DELETE FROM " + SysUser.TableName + " WHERE user_id = ? ", user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "userId", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> editSysUserFunction(long userId) {
        Map<String, Object> editUserInfoMap = new HashMap<>();
        editUserInfoMap.put("departmentList", getDepartmentInfo());
        if (StringUtil.isBlank(String.valueOf(userId))) {
            throw new BusinessException("编辑用户id为空!");
        }
        if (checkSysUserIsNotExist(userId)) {
            throw new BusinessException("编辑的用户id不存在!");
        }
        SysUser editSysUserInfo = getSysUserByUserId(userId);
        editUserInfoMap.put("editSysUserInfo", editSysUserInfo);
        return editUserInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dep_id", desc = "", range = "", example = "", nullable = true)
    @Return(desc = "", range = "")
    private List<DepartmentInfo> getDepartmentInfo() {
        SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
        asmSql.clean();
        asmSql.addSql("select * from " + DepartmentInfo.TableName);
        asmSql.addSql(" order by create_date asc,create_time asc");
        return Dbo.queryList(DepartmentInfo.class, asmSql.sql(), asmSql.params());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "", example = "")
    @Return(desc = "", range = "")
    private boolean checkSysUserIsNotExist(long user_id) {
        return Dbo.queryNumber("SELECT COUNT(1) FROM " + SysUser.TableName + " WHERE user_id = ?", user_id).orElseThrow(() -> new BusinessException("检查系统用户否存在的SQL编写错误")) != 1;
    }
}
