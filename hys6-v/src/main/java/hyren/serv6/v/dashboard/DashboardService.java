package hyren.serv6.v.dashboard;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.InterfaceState;
import hyren.serv6.base.codes.InterfaceType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.AutoDashboardInfo;
import hyren.serv6.base.entity.InterfaceInfo;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.v.dashboard.req.DashboardDataReq;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
public class DashboardService {

    public Map<String, Object> gainDashboardList(Integer pageNum, Integer pageSize) {
        DefaultPageImpl page = new DefaultPageImpl(pageNum, pageSize);
        List<Map<String, Object>> mapList = SqlOperator.queryPagedList(Dbo.db(), page, "SELECT" + " dashboard_id AS id,dashboard_name AS title,create_date,create_time,user_id,dashboard_status FROM " + AutoDashboardInfo.TableName);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("dashboardList", mapList);
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    public Map<String, Object> gainDashboardListByName(String dashboard, Integer pageNum, Integer pageSize) {
        DefaultPageImpl page = new DefaultPageImpl(pageNum, pageSize);
        String sqlStr = new String();
        sqlStr = "SELECT dashboard_id AS id,dashboard_name AS title,create_date,create_time,user_id,dashboard_status FROM " + AutoDashboardInfo.TableName;
        if (dashboard != null) {
            sqlStr += " WHERE dashboard_name like '" + dashboard + "'";
        }
        List<Map<String, Object>> mapList = SqlOperator.queryPagedList(Dbo.db(), page, sqlStr);
        Map<String, Object> resultMap = new HashMap<String, Object>();
        resultMap.put("dashboardList", mapList);
        resultMap.put("totalSize", page.getTotalSize());
        return resultMap;
    }

    public void saveDashboardData(AutoDashboardInfo dashboardInfo, List<Object> widget) {
        Validator.notBlank(dashboardInfo.getDashboard_name(), "仪表盘名称为空，请填写");
        DatabaseWrapper db = Dbo.db();
        List<Map<String, Object>> mapList = SqlOperator.queryList(db, "SELECT dashboard_id,dashboard_name FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_name like '%" + dashboardInfo.getDashboard_name() + "%'");
        if (!mapList.isEmpty()) {
            throw new BusinessException("大屏标题重复,请修改");
        }
        dashboardInfo.setDashboard_id(PrimayKeyGener.getNextId());
        dashboardInfo.setUser_id(UserUtil.getUserId());
        dashboardInfo.setUpdate_user(UserUtil.getUserId());
        dashboardInfo.setCreate_date(DateUtil.getSysDate());
        dashboardInfo.setCreate_time(DateUtil.getSysTime());
        dashboardInfo.setLast_update_date(DateUtil.getSysDate());
        dashboardInfo.setLast_update_time(DateUtil.getSysTime());
        dashboardInfo.setDashboard_status(IsFlag.Fou.getCode());
        String widgetJson = JsonUtil.toJson(widget);
        dashboardInfo.setDashboard_widget(widgetJson);
        dashboardInfo.add(db);
    }

    public void editDashboardData(AutoDashboardInfo autoDashboardInfo, List<Object> widget) {
        Validator.notBlank(autoDashboardInfo.getDashboard_name(), "仪表盘名称为空，请填写");
        Validator.notBlank(autoDashboardInfo.getDashboard_status(), "仪表盘状态不能空");
        if (autoDashboardInfo.getDashboard_id() == null) {
            throw new BusinessException("大屏id为null,请检查");
        }
        if (autoDashboardInfo.getDashboard_status().equals(IsFlag.Shi.getCode())) {
            throw new BusinessException("接口以发布,无法使用编辑");
        }
        List<Map<String, Object>> mapList = SqlOperator.queryList(Dbo.db(), "SELECT dashboard_id,dashboard_name FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_id = ?", autoDashboardInfo.getDashboard_id());
        if (mapList.size() != 1) {
            throw new BusinessException("根据大屏id查询出多条信息或没查询到大屏信息,请检查");
        }
        autoDashboardInfo.setUpdate_user(UserUtil.getUserId());
        autoDashboardInfo.setLast_update_date(DateUtil.getSysDate());
        autoDashboardInfo.setLast_update_time(DateUtil.getSysTime());
        String widgetJson = JsonUtil.toJson(widget);
        autoDashboardInfo.setDashboard_widget(widgetJson);
        autoDashboardInfo.update(Dbo.db());
    }

    public void deleteDashboardData(Long dashboardId) {
        if (dashboardId == null) {
            throw new BusinessException("大屏id为null,请检查");
        }
        Map<String, Object> map = SqlOperator.queryOneObject(Dbo.db(), "SELECT dashboard_id,dashboard_status FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_id = ?", dashboardId);
        String status = map.get("dashboard_status").toString();
        if (!status.equals("") && status != null) {
            if (status.equals(IsFlag.Shi.getCode())) {
                throw new BusinessException("大屏信息以发布,无法删除");
            } else {
                int execute = SqlOperator.execute(Dbo.db(), "DELETE FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_id = ?", dashboardId);
                if (execute != 1) {
                    throw new BusinessException("删除失败");
                }
            }
        } else {
            throw new BusinessException("获取大屏发布状态错误");
        }
    }

    public DashboardDataReq gainDashboardData(Long dashboardId) {
        if (dashboardId == null) {
            throw new BusinessException("大屏id为null,请检查");
        }
        List<Map<String, Object>> mapList = SqlOperator.queryList(Dbo.db(), "SELECT dashboard_id,dashboard_name,border_width,border_height," + "dashboard_desc,background,dashboard_theme,dashboard_status," + "dashboard_widget FROM " + AutoDashboardInfo.TableName + " " + "WHERE dashboard_id = ?", dashboardId);
        if (mapList != null && !mapList.isEmpty()) {
            DashboardDataReq dashboardDataRes = new DashboardDataReq();
            Map<String, Object> map = mapList.get(0);
            dashboardDataRes.setAutoDashboardInfo(map);
            if (map.get("dashboard_widget").toString() != null && !map.get("dashboard_widget").toString().isEmpty()) {
                List<Object> widgets = JsonUtil.toObject(map.get("dashboard_widget").toString(), new TypeReference<List<Object>>() {
                });
                dashboardDataRes.setWidget(widgets);
            }
            return dashboardDataRes;
        }
        return null;
    }

    public void releaseDashboardInfo(long dashboard_id, String title) {
        List<Map<String, Object>> mapList = SqlOperator.queryList(Dbo.db(), "SELECT dashboard_id,dashboard_status FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_id = ?", dashboard_id);
        if (mapList.size() != 1) {
            throw new BusinessException("查询多条大屏信息或未查询到大屏信息");
        }
        String status = mapList.get(0).get("dashboard_status").toString();
        if (status.equals(IsFlag.Shi.getCode())) {
            DboExecute.updatesOrThrow("更新仪表盘盘发布状态失败", "update " + AutoDashboardInfo.TableName + " set dashboard_status=? where dashboard_id=?", IsFlag.Fou.getCode(), dashboard_id);
            String interface_code = Base64.getEncoder().encodeToString(String.valueOf(dashboard_id).getBytes());
            Map<String, Object> interfaceMap = Dbo.queryOneObject("SELECT * FROM " + InterfaceInfo.TableName + " WHERE interface_code = ?", interface_code);
            if (!interfaceMap.isEmpty()) {
                int execute = Dbo.execute("DELETE FROM " + InterfaceInfo.TableName + " WHERE interface_code = ?", interface_code);
                if (execute != 1) {
                    throw new BusinessException("撤销发布失败");
                }
            }
        } else {
            DboExecute.updatesOrThrow("更新仪表盘盘发布状态失败", "update " + AutoDashboardInfo.TableName + " set dashboard_status=? where dashboard_id=?", IsFlag.Shi.getCode(), dashboard_id);
            String interface_code = Base64.getEncoder().encodeToString(String.valueOf(dashboard_id).getBytes());
            Map<String, Object> interfaceMap = Dbo.queryOneObject("SELECT * FROM " + InterfaceInfo.TableName + " WHERE interface_code = ?", interface_code);
            if (interfaceMap.isEmpty()) {
                InterfaceInfo interface_info = new InterfaceInfo();
                interface_info.setInterface_id(PrimayKeyGener.getNextId());
                interface_info.setUser_id(UserUtil.getUserId());
                interface_info.setInterface_code(interface_code);
                interface_info.setInterface_name(title);
                interface_info.setInterface_state(InterfaceState.QiYong.getCode());
                interface_info.setInterface_type(InterfaceType.BaoBiaoLei.getCode());
                interface_info.setUrl(Constant.DASHBOARDINTERFACENAME);
                interface_info.add(Dbo.db());
            } else {
                InterfaceInfo interface_info = JsonUtil.toObjectSafety(JsonUtil.toJson(interfaceMap), InterfaceInfo.class).orElseThrow(() -> new BusinessException("转换接口信息表实体对象失败"));
                interface_info.setInterface_name(title);
                try {
                    interface_info.update(Dbo.db());
                } catch (Exception e) {
                    if (!(e instanceof ProEntity.EntityDealZeroException)) {
                        log.error(String.valueOf(e));
                        throw new BusinessException("更新接口信息失败" + e.getMessage());
                    }
                }
            }
        }
    }
}
