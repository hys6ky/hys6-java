package hyren.serv6.g.releasemanage;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.InterfaceType;
import hyren.serv6.base.codes.MenuType;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.g.init.InterfaceManager;
import org.springframework.stereotype.Service;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class ReleaseManageService {

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public Result searchUserInfo() {
        return Dbo.queryResult("SELECT distinct user_id,user_name,user_email,user_remark FROM " + SysUser.TableName + " t1" + " JOIN " + SysRole.TableName + " t2 on t1.role_id = t2.role_id" + " JOIN " + RoleMenu.TableName + " t3 ON t2.role_id = t3.role_id " + " JOIN " + ComponentMenu.TableName + " t4 ON t3.menu_id = t4.menu_id where menu_type = ?", MenuType.CaoZhuoYuan.getCode());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "interface_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<InterfaceInfo> searchInterfaceInfoByType(String interface_type) {
        InterfaceType.ofEnumByCode(interface_type);
        return Dbo.queryList(InterfaceInfo.class, "SELECT * FROM " + InterfaceInfo.TableName + " WHERE interface_type=? order by interface_id", interface_type);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "interfaceUses", desc = "", range = "", isBean = true)
    @Param(name = "userIds", desc = "", range = "")
    @Param(name = "interface_note", desc = "", range = "", nullable = true)
    @Param(name = "classify_name", desc = "", range = "", nullable = true)
    public void saveInterfaceUseInfo(InterfaceUse[] interfaceUses, Long[] userIds, String interface_note, String classify_name) {
        if (userIds == null || userIds.length == 0) {
            throw new BusinessException("用户ID不能为空");
        }
        for (long userId : userIds) {
            for (InterfaceUse interface_use : interfaceUses) {
                String start_use_date = interface_use.getStart_use_date();
                String use_valid_date = interface_use.getUse_valid_date();
                Long interface_id = interface_use.getInterface_id();
                Validator.notBlank(start_use_date, "开始日期不能为空");
                Validator.notBlank(use_valid_date, "结束日期不能为空");
                Validator.notNull(interface_use.getInterface_id(), "接口ID不能为空");
                Validator.notNull(interface_use.getUrl(), "接口地址不能为空");
                Validator.notNull(interface_use.getInterface_name(), "接口名称不能为空");
                long startUseDate = Long.parseLong(start_use_date);
                long useValidDate = Long.parseLong(use_valid_date);
                long todayDate = Long.parseLong(DateUtil.getSysDate());
                if (startUseDate > useValidDate || useValidDate < todayDate) {
                    throw new BusinessException("开始日期不能大于结束日期，结束日期不能小于当天日期");
                }
                Result interfaceUseResult = Dbo.queryResult("SELECT * FROM " + InterfaceUse.TableName + " WHERE user_id=? AND interface_id=?", userId, interface_id);
                Result interfaceInfoResult = Dbo.queryResult("SELECT * FROM " + InterfaceInfo.TableName + " WHERE interface_id=?", interface_id);
                if (interfaceInfoResult.isEmpty()) {
                    throw new BusinessException("当前接口ID对应的接口信息不存在，请检查，interface_id=" + interface_id);
                }
                interface_use.setUse_state(interfaceInfoResult.getString(0, "interface_state"));
                interface_use.setStart_use_date(start_use_date);
                interface_use.setUse_valid_date(use_valid_date);
                if (interfaceUseResult.isEmpty()) {
                    interface_use.setInterface_use_id(PrimayKeyGener.getNextId());
                    List<String> userNameList = Dbo.queryOneColumnList("SELECT user_name FROM " + SysUser.TableName + " WHERE user_id = ?", userId);
                    if (userNameList.isEmpty()) {
                        throw new BusinessException("当前用户对应用户信息已不存在，user_id=" + userId);
                    }
                    interface_use.setUser_id(userId);
                    interface_use.setTheir_type(interfaceInfoResult.getString(0, "interface_type"));
                    interface_use.setInterface_id(interface_id);
                    interface_use.setUser_name(userNameList.get(0));
                    interface_use.setInterface_note(interface_note);
                    interface_use.setClassify_name(classify_name);
                    interface_use.setCreate_id(UserUtil.getUserId());
                    interface_use.add(Dbo.db());
                } else {
                    Dbo.execute("UPDATE " + InterfaceUse.TableName + " SET use_valid_date=?," + "start_use_date=?,use_state=?,interface_note=?,create_id=?," + "classify_name=? WHERE user_id=? AND interface_id=?", interface_use.getUse_valid_date(), interface_use.getStart_use_date(), interface_use.getUse_state(), interface_note, UserUtil.getUserId(), classify_name, userId, interface_id);
                }
            }
        }
        InterfaceManager.initInterface(Dbo.db());
    }

    public List<Map<String, Object>> queryUseByUserIDs(List<Long> ids) {
        List<Map<String, Object>> resultList = new ArrayList<Map<String, Object>>();
        if (ids.size() > 0) {
            for (Long id : ids) {
                List<Map<String, Object>> list = Dbo.queryList(Dbo.db(), "SELECT * FROM " + InterfaceUse.TableName + " WHERE user_id=?", id);
                resultList.addAll(list);
            }
        }
        return resultList;
    }
}
