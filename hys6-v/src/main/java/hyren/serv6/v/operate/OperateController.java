package hyren.serv6.v.operate;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.resultset.Result;
import hyren.serv6.base.entity.AutoFetchSum;
import hyren.serv6.v.operate.req.AutoAccessInfoReq;
import hyren.serv6.v.operate.req.ChartShowReq;
import hyren.serv6.v.operate.req.ComponentReq;
import hyren.serv6.v.operate.req.VisualComponentInfoReq;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/dataVisualization/operate/")
@Slf4j
@Api(tags = "")
public class OperateController {

    @Autowired
    private OperateService operateService;

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessTemplateInfo")
    public Map<String, Object> getAccessTemplateInfo(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getAccessTemplateInfo(currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_name", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessTemplateInfoByName")
    public Map<String, Object> getAccessTemplateInfoByName(String template_name, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getAccessTemplateInfoByName(template_name, currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessTemplateInfoById")
    public Map<String, Object> getAccessTemplateInfoById(long template_id) {
        return operateService.getAccessTemplateInfoById(template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessResultFields")
    public List<Map<String, Object>> getAccessResultFields(long template_id) {
        return operateService.getAccessResultFields(template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAutoAccessFilterCond")
    public List<Map<String, Object>> getAutoAccessFilterCond(long template_id) {
        return operateService.getAutoAccessFilterCond(template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessSelectHistory")
    public List<Map<String, Object>> getAccessSelectHistory(long template_id) {
        return operateService.getAccessSelectHistory(template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessCondFromHistory")
    public List<Map<String, Object>> getAccessCondFromHistory(long fetch_sum_id) {
        return operateService.getAccessCondFromHistory(fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessResultFromHistory")
    public List<Map<String, Object>> getAccessResultFromHistory(long fetch_sum_id) {
        return operateService.getAccessResultFromHistory(fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAutoAccessQueryResult")
    public List<Map<String, Object>> getAutoAccessQueryResult(long fetch_sum_id) {
        return operateService.getAutoAccessQueryResult(fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_fetch_sum", desc = "", range = "", isBean = true)
    @Param(name = "autoTpCondInfos", desc = "", range = "", isBean = true)
    @Param(name = "autoFetchRes", desc = "", range = "", isBean = true)
    @PostMapping("/saveAutoAccessInfoToQuery")
    public Long saveAutoAccessInfoToQuery(@RequestBody AutoAccessInfoReq autoAccessInfoReq) {
        return operateService.saveAutoAccessInfoToQuery(autoAccessInfoReq.getAuto_fetch_sum(), autoAccessInfoReq.getAutoTpCondInfos(), autoAccessInfoReq.getAutoFetchRes());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_fetch_sum", desc = "", range = "", isBean = true)
    @PostMapping("/saveAutoAccessInfo")
    public void saveAutoAccessInfo(@RequestBody AutoFetchSum auto_fetch_sum) {
        operateService.saveAutoAccessInfo(auto_fetch_sum);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getMyAccessInfo")
    public Map<String, Object> getMyAccessInfo(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getMyAccessInfo(currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_name", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getMyAccessInfoByName")
    public Map<String, Object> getMyAccessInfoByName(String fetch_name, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getMyAccessInfoByName(fetch_name, currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getMyAccessInfoById")
    public Map<String, Object> getMyAccessInfoById(long fetch_sum_id) {
        return operateService.getMyAccessInfoById(fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessResultByNumber")
    public List<Map<String, Object>> getAccessResultByNumber(Long fetch_sum_id, Integer showNum) {
        if (showNum == null) {
            showNum = -1;
        }
        return operateService.getAccessResultByNumber(fetch_sum_id, showNum);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAccessSql")
    public String getAccessSql(long fetch_sum_id) {
        return operateService.getAccessSql(fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @PostMapping("/downloadMyAccessTemplate")
    public void downloadMyAccessTemplate(long fetch_sum_id) {
        operateService.downloadMyAccessTemplate(fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @RequestMapping("/getVisualComponentInfo")
    public Map<String, Object> getVisualComponentInfo(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getVisualComponentInfo(currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "componentName", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @RequestMapping("/getVisualComponentInfoByName")
    public Map<String, Object> getVisualComponentInfoByName(@RequestParam(name = "componentName") String componentName, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getVisualComponentInfoByName(componentName, currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "data_source", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getColumnByName")
    public Map<String, Object> getColumnByName(String table_name, String data_source) {
        return operateService.getColumnByName(table_name, data_source);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_id", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/getVisualComponentInfoById")
    public Map<String, Object> getVisualComponentInfoById(long component_id) {
        return operateService.getVisualComponentInfoById(component_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "exe_sql", desc = "", range = "")
    @Param(name = "x_columns", desc = "", range = "", nullable = true)
    @Param(name = "y_columns", desc = "", range = "", nullable = true)
    @Param(name = "chart_type", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "50")
    @Return(desc = "", range = "")
    @PostMapping("/getChartShow")
    public Map<String, Object> getChartShow(@RequestBody ChartShowReq chartShowReq) {
        Integer showNum = 50;
        if (chartShowReq.getShowNum() != null) {
            showNum = chartShowReq.getShowNum();
        }
        return operateService.getChartShow(chartShowReq.getExe_sql(), chartShowReq.getX_columns(), chartShowReq.getY_columns(), chartShowReq.getChart_type(), showNum);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "exe_sql", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "50")
    @Return(desc = "", range = "")
    @PostMapping("/getVisualComponentResult")
    public Map<String, Object> getVisualComponentResult(String exe_sql, @RequestParam(defaultValue = "-1") Integer showNum) {
        return operateService.getVisualComponentResult(exe_sql, showNum);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "componentBean", desc = "", range = "", isBean = true)
    @Param(name = "autoCompConds", desc = "", range = "", isBean = true)
    @Param(name = "autoCompGroups", desc = "", range = "", isBean = true)
    @Param(name = "autoCompDataSums", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    @PostMapping("/getSqlByCondition")
    public String getSqlByCondition(@RequestBody ComponentReq componentReq) {
        return operateService.getSqlByCondition(componentReq.getComponentBean(), componentReq.getAutoCompConds(), componentReq.getAutoCompGroups(), componentReq.getAutoCompDataSums());
    }

    @Param(name = "componentBeanString", desc = "", range = "")
    @Param(name = "auto_comp_sumString", desc = "", range = "")
    @Param(name = "autoCompCondString", desc = "", range = "", nullable = true)
    @Param(name = "autoCompGroupString", desc = "", range = "", nullable = true)
    @Param(name = "autoCompDataSumString", desc = "", range = "")
    @Param(name = "titleFontString", desc = "", range = "", nullable = true)
    @Param(name = "axisStyleFontString", desc = "", range = "", nullable = true)
    @Param(name = "autoAxisInfoString", desc = "", range = "", nullable = true)
    @Param(name = "xAxisLabelString", desc = "", range = "", nullable = true)
    @Param(name = "yAxisLabelString", desc = "", range = "", nullable = true)
    @Param(name = "xAxisLineString", desc = "", range = "", nullable = true)
    @Param(name = "yAxisLineString", desc = "", range = "", nullable = true)
    @Param(name = "auto_table_infoString", desc = "", range = "", nullable = true)
    @Param(name = "auto_chartsconfigString", desc = "", range = "", nullable = true)
    @Param(name = "auto_labelString", desc = "", range = "", nullable = true)
    @Param(name = "auto_legend_infoString", desc = "", range = "", nullable = true)
    @Param(name = "allcolumn", desc = "", range = "", nullable = true)
    @PostMapping("/updateVisualComponentInfo")
    public Long updateVisualComponentInfo(String componentBeanString, String auto_comp_sumString, String autoCompCondString, String autoCompGroupString, String autoCompDataSumString, String titleFontString, String axisStyleFontString, String autoAxisInfoString, String xAxisLabelString, String yAxisLabelString, String xAxisLineString, String yAxisLineString, String auto_table_infoString, String auto_chartsconfigString, String auto_labelString, String auto_legend_infoString, String allcolumn) {
        return operateService.updateVisualComponentInfo(componentBeanString, auto_comp_sumString, autoCompCondString, autoCompGroupString, autoCompDataSumString, titleFontString, axisStyleFontString, autoAxisInfoString, xAxisLabelString, yAxisLabelString, xAxisLineString, yAxisLineString, auto_table_infoString, auto_chartsconfigString, auto_labelString, auto_legend_infoString, allcolumn);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "componentBeanString", desc = "", range = "")
    @Param(name = "auto_comp_sumString", desc = "", range = "")
    @Param(name = "autoCompCondString", desc = "", range = "", nullable = true)
    @Param(name = "autoCompGroupString", desc = "", range = "", nullable = true)
    @Param(name = "autoCompDataSumString", desc = "", range = "")
    @Param(name = "titleFontString", desc = "", range = "", nullable = true)
    @Param(name = "axisStyleFontString", desc = "", range = "", nullable = true)
    @Param(name = "autoAxisInfoString", desc = "", range = "", nullable = true)
    @Param(name = "xAxisLabelString", desc = "", range = "", nullable = true)
    @Param(name = "yAxisLabelString", desc = "", range = "", nullable = true)
    @Param(name = "xAxisLineString", desc = "", range = "", nullable = true)
    @Param(name = "yAxisLineString", desc = "", range = "", nullable = true)
    @Param(name = "auto_table_infoString", desc = "", range = "", nullable = true)
    @Param(name = "auto_chartsconfigString", desc = "", range = "", nullable = true)
    @Param(name = "auto_labelString", desc = "", range = "", nullable = true)
    @Param(name = "auto_legend_infoString", desc = "", range = "", nullable = true)
    @PostMapping("/addVisualComponentInfo")
    public Long addVisualComponentInfo(VisualComponentInfoReq visualComponentInfoReq) {
        return operateService.addVisualComponentInfo(visualComponentInfoReq.getComponentBeanString(), visualComponentInfoReq.getAuto_comp_sumString(), visualComponentInfoReq.getAutoCompCondString(), visualComponentInfoReq.getAutoCompGroupString(), visualComponentInfoReq.getAutoCompDataSumString(), visualComponentInfoReq.getTitleFontString(), visualComponentInfoReq.getAxisStyleFontString(), visualComponentInfoReq.getAutoAxisInfoString(), visualComponentInfoReq.getXAxisLabelString(), visualComponentInfoReq.getYAxisLabelString(), visualComponentInfoReq.getXAxisLineString(), visualComponentInfoReq.getYAxisLineString(), visualComponentInfoReq.getAuto_table_infoString(), visualComponentInfoReq.getAuto_chartsconfigString(), visualComponentInfoReq.getAuto_labelString(), visualComponentInfoReq.getAuto_legend_infoString());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_id", desc = "", range = "")
    @PostMapping("/deleteVisualComponent")
    public void deleteVisualComponent(long component_id) {
        operateService.deleteVisualComponent(component_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getDataDashboardInfo")
    public Map<String, Object> getDataDashboardInfo(@RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getDataDashboardInfo(currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dashboardName", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    @PostMapping("/getDataDashboardInfoByName")
    public Map<String, Object> getDataDashboardInfoByName(String dashboardName, @RequestParam(defaultValue = "1") Integer currPage, @RequestParam(defaultValue = "10") Integer pageSize) {
        return operateService.getDataDashboardInfoByName(dashboardName, currPage, pageSize);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoCompSums", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/showComponentOnDashboard")
    public Map<String, Object> showComponentOnDashboard(String autoCompSums) {
        return operateService.showComponentOnDashboard(autoCompSums);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoCompSums", desc = "", range = "")
    @Return(desc = "", range = "")
    @PostMapping("/showComponentData")
    public Map<Long, Object> showComponentData(@RequestBody List<Long> component_ids) {
        return operateService.showComponentData(component_ids);
    }

    @RequestMapping("/getCategoryItems")
    public Result getCategoryItems(String category) {
        return operateService.getCategoryItems(category);
    }
}
