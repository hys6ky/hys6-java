package hyren.serv6.v.common;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AxisType;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.v.dashboard.req.DashboardDataReq;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2020/10/20 17:24")
public class AutoAnalysisUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "dashboard_id", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DashboardDataReq getDashboardInfoById(long dashboard_id) {
        Map<String, Object> dashboardInfo = SqlOperator.queryOneObject(Dbo.db(), "SELECT * FROM " + AutoDashboardInfo.TableName + " WHERE dashboard_id=?", dashboard_id);
        if (dashboardInfo != null && !dashboardInfo.isEmpty()) {
            DashboardDataReq dataReq = new DashboardDataReq();
            dataReq.setAutoDashboardInfo(dashboardInfo);
            String dashboardWidget = dashboardInfo.get("dashboard_widget").toString();
            if (dashboardWidget != null && !dashboardWidget.isEmpty()) {
                List<Object> widgets = JsonUtil.toObject(dashboardWidget, new TypeReference<List<Object>>() {
                });
                dataReq.setWidget(widgets);
            }
            return dataReq;
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_id", desc = "", range = "")
    @Param(name = "db", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> getVisualComponentInfoById(long component_id, DatabaseWrapper db) {
        Map<String, Object> resultMap = new HashMap<>();
        AutoCompSum auto_comp_sum = SqlOperator.queryOneObject(db, AutoCompSum.class, "SELECT * FROM " + AutoCompSum.TableName + " WHERE component_id = ?", component_id).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
        resultMap.put("compSum", auto_comp_sum);
        List<Map<String, Object>> compCondList = SqlOperator.queryList(db, "SELECT * FROM " + AutoCompCond.TableName + " WHERE component_id = ?", component_id);
        resultMap.put("compCond", JsonUtil.toObject(JsonUtil.toJson(compCondList), List.class));
        List<Map<String, Object>> compGroupList = SqlOperator.queryList(db, "SELECT * FROM " + AutoCompGroup.TableName + " WHERE component_id = ?", component_id);
        resultMap.put("compGroup", JsonUtil.toObject(JsonUtil.toJson(compGroupList), List.class));
        List<Map<String, Object>> compDataSumList = SqlOperator.queryList(db, "SELECT * FROM " + AutoCompDataSum.TableName + " WHERE component_id = ?", component_id);
        resultMap.put("compDataSum", compDataSumList);
        List<Map<String, Object>> xAxisColList = SqlOperator.queryList(db, "SELECT * FROM " + AutoAxisColInfo.TableName + " WHERE component_id = ? AND show_type = ?", component_id, AxisType.XAxis.getCode());
        resultMap.put("xAxisCol", JsonUtil.toObject(JsonUtil.toJson(xAxisColList), List.class));
        List<Map<String, Object>> yAxisColList = SqlOperator.queryList(db, "SELECT * FROM " + AutoAxisColInfo.TableName + " WHERE component_id = ? AND show_type = ?", component_id, AxisType.YAxis.getCode());
        resultMap.put("yAxisCol", JsonUtil.toObject(JsonUtil.toJson(yAxisColList), List.class));
        Map<String, Object> fontInfoMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoFontInfo.TableName + " WHERE font_corr_id = ?", component_id);
        resultMap.put("titleFontInfo", JsonUtil.toObject(JsonUtil.toJson(fontInfoMap), Map.class));
        Map<String, Object> xFontInfoMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoFontInfo.TableName + " WHERE font_corr_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?)", component_id, AxisType.XAxis.getCode());
        resultMap.put("axisFontInfo", JsonUtil.toObject(JsonUtil.toJson(xFontInfoMap), Map.class));
        List<Map<String, Object>> xAxisInfoList = SqlOperator.queryList(db, "SELECT * FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?", component_id, AxisType.XAxis.getCode());
        resultMap.put("xAxisInfo", JsonUtil.toObject(JsonUtil.toJson(xAxisInfoList), List.class));
        List<Map<String, Object>> yAxisInfoList = SqlOperator.queryList(db, "SELECT * FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?", component_id, AxisType.YAxis.getCode());
        resultMap.put("yAxisInfo", JsonUtil.toObject(JsonUtil.toJson(yAxisInfoList), List.class));
        Map<String, Object> xAxislabelMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoAxislabelInfo.TableName + " WHERE axis_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?)", component_id, AxisType.XAxis.getCode());
        resultMap.put("xAxisLabel", JsonUtil.toObject(JsonUtil.toJson(xAxislabelMap), Map.class));
        Map<String, Object> yAxislabelMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoAxislabelInfo.TableName + " WHERE axis_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?)", component_id, AxisType.YAxis.getCode());
        resultMap.put("yAxisLabel", JsonUtil.toObject(JsonUtil.toJson(yAxislabelMap), Map.class));
        Map<String, Object> xAxislineMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoAxislineInfo.TableName + " WHERE axis_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?)", component_id, AxisType.XAxis.getCode());
        resultMap.put("xAxisLine", JsonUtil.toObject(JsonUtil.toJson(xAxislineMap), Map.class));
        Map<String, Object> yAxislineMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoAxislineInfo.TableName + " WHERE axis_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ? AND axis_type = ?)", component_id, AxisType.YAxis.getCode());
        resultMap.put("yAxisLine", JsonUtil.toObject(JsonUtil.toJson(yAxislineMap), Map.class));
        Map<String, Object> tableInfoMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoTableInfo.TableName + " WHERE component_id = ?", component_id);
        resultMap.put("twoDimensionalTable", JsonUtil.toObject(JsonUtil.toJson(tableInfoMap), Map.class));
        Map<String, Object> chartsconfigMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoChartsconfig.TableName + " WHERE component_id = ?", component_id);
        resultMap.put("chartsconfig", JsonUtil.toObject(JsonUtil.toJson(chartsconfigMap), Map.class));
        Map<String, Object> textLabelMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoLabel.TableName + " WHERE label_corr_id = ?", component_id);
        resultMap.put("textLabel", JsonUtil.toObject(JsonUtil.toJson(textLabelMap), Map.class));
        Map<String, Object> legendMap = SqlOperator.queryOneObject(db, "SELECT * FROM " + AutoLegendInfo.TableName + " WHERE component_id = ?", component_id);
        resultMap.put("legendInfo", JsonUtil.toObject(JsonUtil.toJson(legendMap), Map.class));
        return resultMap;
    }
}
