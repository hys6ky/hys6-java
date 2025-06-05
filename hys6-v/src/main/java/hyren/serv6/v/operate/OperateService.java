package hyren.serv6.v.operate;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import fd.ng.db.jdbc.SqlOperator;
import fd.ng.db.resultset.Result;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.*;
import hyren.serv6.base.codes.fdCode.WebCodesItem;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.v.bean.ComponentBean;
import hyren.serv6.v.common.AutoAnalysisUtil;
import hyren.serv6.v.common.AutoOperateCommon;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import java.io.File;
import java.math.BigDecimal;
import java.util.*;

@Service
@Slf4j
public class OperateService {

    private static final Logger logger = LogManager.getLogger();

    private static final String COLUMNNAME = "COLUMN";

    private static final String ZUIDANGE = "ZUIDANGE";

    private static final String ZUIXIAONGE = "ZUIXIAONGE";

    private static final String ZUIDAXIAOKEY = "ZUIDAXIAOKEY";

    private static final String LIMITVALUE = "LIMITVALUE";

    private static final String TempTableName = " TEMP_TABLE ";

    private static final String LINE = "line";

    private static final String BAR = "bar";

    private static final String STACKINGBAR = "stackingbar";

    private static final String BL = "bl";

    private static final String BLSIMPLE = "blsimple";

    private static final String POLARBAR = "polarbar";

    private static final String SCATTER = "scatter";

    private static final String BUBBLE = "bubble";

    private static final String PIE = "pie";

    private static final String HUANPIE = "huanpie";

    private static final String FASANPIE = "fasanpie";

    private static final String CARD = "card";

    private static final String TABLE = "table";

    private static final String TREEMAP = "treemap";

    private static final String MAP = "map";

    private static final String LargeScreenTheme = "LargeScreenTheme";

    private static final ArrayList<String> numbersArray = new ArrayList<>();

    static {
        numbersArray.add("int");
        numbersArray.add("int8");
        numbersArray.add("int16");
        numbersArray.add("int4");
        numbersArray.add("integer");
        numbersArray.add("tinyint");
        numbersArray.add("smallint");
        numbersArray.add("mediumint");
        numbersArray.add("bigint");
        numbersArray.add("float");
        numbersArray.add("double");
        numbersArray.add("decimal");
        numbersArray.add("numeric");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getVisualComponentInfo(Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> visualCompList = Dbo.queryPagedList(page, "SELECT * FROM " + AutoCompSum.TableName + " WHERE create_user = ? AND component_name not like '%-other' order by create_date desc,create_time desc", UserUtil.getUserId());
        Map<String, Object> tpInfoMap = new HashMap<>();
        tpInfoMap.put("visualCompList", visualCompList);
        tpInfoMap.put("totalSize", page.getTotalSize());
        return tpInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "componentName", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getVisualComponentInfoByName(String componentName, Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> visualCompList = Dbo.queryPagedList(page, "SELECT * FROM " + AutoCompSum.TableName + " WHERE create_user = ? and component_name like ?" + " order by create_date desc,create_time desc", UserUtil.getUserId(), "%" + componentName + "%");
        Map<String, Object> visualCompMap = new HashMap<>();
        visualCompMap.put("visualCompList", visualCompList);
        visualCompMap.put("totalSize", page.getTotalSize());
        return visualCompMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_id", desc = "", range = "")
    public void deleteVisualComponent(long component_id) {
        DboExecute.deletesOrThrow("删除可视化组件信息失败", "DELETE FROM " + AutoCompSum.TableName + " WHERE component_id = ?", component_id);
        deleteComponentAssociateTable(component_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_id", desc = "", range = "")
    private void deleteComponentAssociateTable(long component_id) {
        Dbo.execute("DELETE FROM " + AutoCompCond.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoCompGroup.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoCompDataSum.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoAxisColInfo.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoFontInfo.TableName + " WHERE font_corr_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoFontInfo.TableName + " WHERE font_corr_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ?)", component_id);
        Dbo.execute("DELETE FROM " + AutoAxislabelInfo.TableName + " WHERE axis_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ?)", component_id);
        Dbo.execute("DELETE FROM " + AutoAxislineInfo.TableName + " WHERE axis_id IN (SELECT axis_id FROM " + AutoAxisInfo.TableName + " WHERE component_id = ?)", component_id);
        Dbo.execute("DELETE FROM " + AutoAxisInfo.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoTableInfo.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoChartsconfig.TableName + " WHERE component_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoLabel.TableName + " WHERE label_corr_id = ?", component_id);
        Dbo.execute("DELETE FROM " + AutoLegendInfo.TableName + " WHERE component_id = ?", component_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getMyAccessInfo(Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> myAccessList = Dbo.queryPagedList(page, "select * from " + AutoFetchSum.TableName + " where create_user = ?" + " and fetch_name !='' order by create_date desc,create_time desc", UserUtil.getUserId());
        Map<String, Object> myAccessMap = new HashMap<>();
        myAccessMap.put("myAccessList", myAccessList);
        myAccessMap.put("totalSize", page.getTotalSize());
        return myAccessMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "data_source", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getColumnByName(String table_name, String data_source) {
        Map<String, Object> columnMap = new HashMap<>();
        List<Map<String, Object>> numColumnList = new ArrayList<>();
        if (AutoSourceObject.ZiZhuShuJuShuJuJi == AutoSourceObject.ofEnumByCode(data_source)) {
            List<Map<String, Object>> columnList = Dbo.queryList("SELECT t1.fetch_res_name,t3.column_type FROM " + AutoFetchRes.TableName + " t1 left join " + AutoFetchSum.TableName + " t2 on t1.fetch_sum_id = t2.fetch_sum_id" + " left join " + AutoTpResSet.TableName + " t3 on t1.template_res_id = t3.template_res_id WHERE t2.fetch_name = ?" + " AND t2.fetch_status = ? order by t1.show_num", table_name, AutoFetchStatus.WanCheng.getCode());
            for (Map<String, Object> map : columnList) {
                if (AutoValueType.ShuZhi == AutoValueType.ofEnumByCode(map.get("column_type").toString())) {
                    if (!numColumnList.contains(map)) {
                        numColumnList.add(map);
                    }
                }
            }
            columnMap.put("columns", columnList);
            columnMap.put("numColumns", numColumnList);
        } else if (AutoSourceObject.XiTongJiShuJuJi == AutoSourceObject.ofEnumByCode(data_source)) {
            List<Map<String, Object>> columnList = DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
            for (Map<String, Object> map : columnList) {
                if (numbersArray.contains(map.get("data_type").toString())) {
                    if (!numColumnList.contains(map)) {
                        numColumnList.add(map);
                    }
                }
            }
            columnMap.put("numColumns", numColumnList);
            columnMap.put("columns", columnList);
        }
        return columnMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "componentBean", desc = "", range = "", isBean = true)
    @Param(name = "autoCompConds", desc = "", range = "", isBean = true)
    @Param(name = "autoCompGroups", desc = "", range = "", isBean = true)
    @Param(name = "autoCompDataSums", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    public String getSqlByCondition(ComponentBean componentBean, AutoCompCond[] autoCompConds, AutoCompGroup[] autoCompGroups, AutoCompDataSum[] autoCompDataSums) {
        Validator.notNull(componentBean.getFetch_name(), "取数名称不能为空");
        Validator.notNull(componentBean.getData_source(), "数据来源不能为空");
        String fetch_sql;
        List<String> databaseTypeList = new ArrayList<>();
        if (AutoSourceObject.ZiZhuShuJuShuJuJi == AutoSourceObject.ofEnumByCode(componentBean.getData_source())) {
            List<Long> idList = Dbo.queryOneColumnList("select fetch_sum_id from " + AutoFetchSum.TableName + " where fetch_name=?", componentBean.getFetch_name());
            fetch_sql = getAccessSql(idList.get(0));
            List<String> sqlTableList = DruidParseQuerySql.getSqlTableList(fetch_sql, DbType.oracle.toString());
            Map<String, List<LayerBean>> layerByTableMap = ProcessingData.getLayerByTable(sqlTableList, Dbo.db());
            for (Map.Entry<String, List<LayerBean>> next : layerByTableMap.entrySet()) {
                List<LayerBean> layerByTableList = next.getValue();
                for (LayerBean layerBean : layerByTableList) {
                    String databaseType = getDatabaseType(layerBean);
                    if (!databaseTypeList.contains(databaseType)) {
                        databaseTypeList.add(databaseType);
                    }
                }
            }
        } else if (AutoSourceObject.XiTongJiShuJuJi == AutoSourceObject.ofEnumByCode(componentBean.getData_source())) {
            List<LayerBean> layerByTableList = ProcessingData.getLayerByTable(componentBean.getFetch_name(), Dbo.db());
            fetch_sql = "SELECT" + Constant.SPACE + "*" + Constant.SPACE + "FROM" + Constant.SPACE + componentBean.getFetch_name();
            for (LayerBean layerBean : layerByTableList) {
                String databaseType = getDatabaseType(layerBean);
                if (!databaseTypeList.contains(databaseType)) {
                    databaseTypeList.add(databaseType);
                }
            }
        } else {
            throw new BusinessException("暂不支持该种数据集" + componentBean.getData_source());
        }
        String seperator = "";
        if (databaseTypeList.size() == 0) {
            throw new BusinessException("表未找到存储位置");
        } else {
            String databaseType = databaseTypeList.get(0);
            if (databaseType.toLowerCase().equals("hive")) {
                seperator = "`";
            }
        }
        StringBuilder result_sql = new StringBuilder();
        result_sql.append("SELECT" + Constant.SPACE);
        Map<String, Object> columnByName = getColumnByName(componentBean.getFetch_name(), componentBean.getData_source());
        List<Map<String, Object>> columnList = JsonUtil.toObject(JsonUtil.toJson(columnByName.get("columns")), new TypeReference<List<Map<String, Object>>>() {
        });
        List<String> allColumnList = new ArrayList<>();
        for (Map<String, Object> column : columnList) {
            String column_name;
            if (AutoSourceObject.ZiZhuShuJuShuJuJi == AutoSourceObject.ofEnumByCode(componentBean.getData_source())) {
                column_name = column.get("fetch_res_name").toString();
            } else {
                column_name = column.get("column_name").toString();
            }
            allColumnList.add(column_name);
        }
        for (AutoCompDataSum auto_comp_data_sum : autoCompDataSums) {
            String selectSql = getSelectSql(auto_comp_data_sum, seperator, allColumnList);
            result_sql.append(selectSql);
        }
        result_sql = new StringBuilder(result_sql.substring(0, result_sql.length() - 1));
        result_sql.append(Constant.SPACE + "FROM (").append(fetch_sql).append(") ").append(Constant.SPACE).append(TempTableName);
        ArrayList<Map<String, String>> upAndLowArray = new ArrayList<>();
        List<AutoCompCond> havingListData = new ArrayList<>();
        boolean firstAppend = true;
        if (autoCompConds != null && autoCompConds.length > 0) {
            for (int i = 0; i < autoCompConds.length; i++) {
                AutoCompCond auto_comp_cond = autoCompConds[i];
                if (auto_comp_cond.getCond_en_column().trim().startsWith("HAVING")) {
                    havingListData.add(auto_comp_cond);
                    continue;
                }
                if (firstAppend) {
                    result_sql.append(Constant.SPACE + "WHERE" + Constant.SPACE);
                    firstAppend = false;
                }
                String condSql = getCondSql(auto_comp_cond, upAndLowArray);
                if (condSql != null) {
                    result_sql.append(condSql).append(Constant.SPACE).append("AND").append(Constant.SPACE).append(Constant.SPACE);
                }
            }
            result_sql = new StringBuilder(result_sql.substring(0, result_sql.length() - 6));
        }
        if (autoCompGroups != null && autoCompGroups.length > 0) {
            result_sql.append(Constant.SPACE + "GROUP BY" + Constant.SPACE);
            for (AutoCompGroup auto_comp_group : autoCompGroups) {
                String column_name = auto_comp_group.getColumn_name();
                result_sql.append(column_name).append(",");
            }
            result_sql = new StringBuilder(result_sql.substring(0, result_sql.length() - 1));
        }
        if (!upAndLowArray.isEmpty()) {
            int[] number = new int[upAndLowArray.size()];
            result_sql.append(Constant.SPACE + "ORDER BY" + Constant.SPACE);
            for (int i = 0; i < upAndLowArray.size(); i++) {
                Map<String, String> map = upAndLowArray.get(i);
                String limitValue = map.get(LIMITVALUE);
                if (!StringUtil.isNumeric(limitValue)) {
                    throw new BusinessException("当前过滤条件:" + map.get(COLUMNNAME) + "的值不是数字");
                }
                number[i] = Integer.parseInt(map.get(LIMITVALUE));
                result_sql.append(map.get(COLUMNNAME));
                if (map.get(ZUIDAXIAOKEY).equals(ZUIDANGE)) {
                    result_sql.append(Constant.SPACE + "DESC" + Constant.SPACE);
                }
                result_sql.append(",");
            }
            result_sql = new StringBuilder(result_sql.substring(0, result_sql.length() - 1));
            Arrays.sort(number);
            result_sql.append(Constant.SPACE + "LIMIT").append(Arrays.toString(number));
        }
        if (!havingListData.isEmpty()) {
            String condSql;
            AutoCompCond autoCompCond;
            for (int i = 0; i < havingListData.size(); i++) {
                autoCompCond = havingListData.get(i);
                condSql = getCondSql(autoCompCond, new ArrayList<>());
                if (i == 0) {
                    result_sql.append(Constant.SPACE).append(condSql).append(Constant.SPACE).append("AND").append(Constant.SPACE);
                } else {
                    condSql = StringUtil.replace(condSql, "HAVING", "");
                    result_sql.append(Constant.SPACE).append(condSql).append(Constant.SPACE).append("AND").append(Constant.SPACE);
                }
            }
            result_sql = new StringBuilder(result_sql.substring(0, result_sql.length() - 6));
        }
        return result_sql.toString();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "exe_sql", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "50")
    @Return(desc = "", range = "")
    public Map<String, Object> getVisualComponentResult(String exe_sql, Integer showNum) {
        List<Map<String, Object>> visualComponentList = new ArrayList<>();
        Set<String> columnList = new HashSet<>();
        new ProcessingData() {

            @Override
            public void dealLine(Map<String, Object> map) {
                map.forEach((k, v) -> columnList.add(k));
                visualComponentList.add(map);
            }
        }.getPageDataLayer(exe_sql, Dbo.db(), 1, showNum <= 0 ? 50 : showNum);
        Map<String, Object> visualComponentMap = new HashMap<>();
        visualComponentMap.put("visualComponentList", visualComponentList);
        visualComponentMap.put("columnList", columnList);
        return visualComponentMap;
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
    public Long addVisualComponentInfo(String componentBeanString, String auto_comp_sumString, String autoCompCondString, String autoCompGroupString, String autoCompDataSumString, String titleFontString, String axisStyleFontString, String autoAxisInfoString, String xAxisLabelString, String yAxisLabelString, String xAxisLineString, String yAxisLineString, String auto_table_infoString, String auto_chartsconfigString, String auto_labelString, String auto_legend_infoString) {
        ComponentBean componentBean = JsonUtil.toObject(componentBeanString, new TypeReference<ComponentBean>() {
        });
        AutoCompSum auto_comp_sum = JsonUtil.toObject(auto_comp_sumString, new TypeReference<AutoCompSum>() {
        });
        AutoFontInfo titleFont = JsonUtil.toObject(titleFontString, new TypeReference<AutoFontInfo>() {
        });
        AutoFontInfo axisStyleFont = JsonUtil.toObject(axisStyleFontString, new TypeReference<AutoFontInfo>() {
        });
        AutoAxislabelInfo xAxisLabel = JsonUtil.toObject(xAxisLabelString, new TypeReference<AutoAxislabelInfo>() {
        });
        AutoAxislabelInfo yAxisLabel = JsonUtil.toObject(yAxisLabelString, new TypeReference<AutoAxislabelInfo>() {
        });
        AutoAxislineInfo xAxisLine = JsonUtil.toObject(xAxisLineString, new TypeReference<AutoAxislineInfo>() {
        });
        AutoAxislineInfo yAxisLine = JsonUtil.toObject(yAxisLineString, new TypeReference<AutoAxislineInfo>() {
        });
        AutoTableInfo auto_table_info = JsonUtil.toObject(auto_table_infoString, new TypeReference<AutoTableInfo>() {
        });
        AutoChartsconfig auto_chartsconfig = JsonUtil.toObject(auto_chartsconfigString, new TypeReference<AutoChartsconfig>() {
        });
        AutoLabel auto_label = JsonUtil.toObject(auto_labelString, new TypeReference<AutoLabel>() {
        });
        AutoLegendInfo auto_legend_info = JsonUtil.toObject(auto_legend_infoString, new TypeReference<AutoLegendInfo>() {
        });
        AutoCompCond[] autoCompConds = JsonUtil.toObject(autoCompCondString, new TypeReference<AutoCompCond[]>() {
        });
        AutoCompGroup[] autoCompGroups = JsonUtil.toObject(autoCompGroupString, new TypeReference<AutoCompGroup[]>() {
        });
        AutoCompDataSum[] autoCompDataSums = JsonUtil.toObject(autoCompDataSumString, new TypeReference<AutoCompDataSum[]>() {
        });
        AutoAxisInfo[] autoAxisInfos = JsonUtil.toObject(autoAxisInfoString, new TypeReference<AutoAxisInfo[]>() {
        });
        Validator.notBlank(componentBean.getFetch_name(), "取数名称不能为空");
        checkAutoCompSumFields(auto_comp_sum);
        auto_comp_sum.setCreate_user(UserUtil.getUserId());
        auto_comp_sum.setSources_obj(componentBean.getFetch_name());
        auto_comp_sum.setComponent_id(PrimayKeyGener.getNextId());
        auto_comp_sum.setCreate_date(DateUtil.getSysDate());
        auto_comp_sum.setCreate_time(DateUtil.getSysTime());
        auto_comp_sum.setComponent_status(AutoFetchStatus.WanCheng.getCode());
        String exe_sql = getSqlByCondition(componentBean, autoCompConds, autoCompGroups, autoCompDataSums);
        auto_comp_sum.setExe_sql(exe_sql);
        Map<String, Object> chartShow = getChartShow(exe_sql, componentBean.getX_columns(), componentBean.getY_columns(), auto_comp_sum.getChart_type(), 50);
        chartShow.put("itemStyle", auto_label);
        auto_comp_sum.setComponent_buffer(JsonUtil.toJson(chartShow));
        isAutoCompSumExist(auto_comp_sum.getComponent_name());
        auto_comp_sum.add(Dbo.db());
        saveComponentInfo(componentBean, auto_comp_sum, titleFont, axisStyleFont, xAxisLabel, yAxisLabel, xAxisLine, yAxisLine, auto_chartsconfig, auto_label, auto_legend_info, autoCompConds, autoCompGroups, autoCompDataSums, autoAxisInfos, auto_table_info);
        return auto_comp_sum.getComponent_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getVisualComponentInfoById(long component_id) {
        Map<String, Object> componentInfo = AutoAnalysisUtil.getVisualComponentInfoById(component_id, Dbo.db());
        AutoCompSum auto_comp_sum = JsonUtil.toObjectSafety(JsonUtil.toJson(componentInfo.get("compSum")), AutoCompSum.class).orElseThrow(() -> new BusinessException("实体" + AutoCompSum.TableName + "转换失败"));
        List<Map<String, Object>> xAxisColList = JsonUtil.toObject(JsonUtil.toJson(componentInfo.get("xAxisCol")), new TypeReference<List<Map<String, Object>>>() {
        });
        String[] x_columns = new String[xAxisColList.size()];
        for (int i = 0; i < xAxisColList.size(); i++) {
            x_columns[i] = xAxisColList.get(i).get("column_name").toString();
        }
        List<Map<String, Object>> yAxisColList = JsonUtil.toObject(JsonUtil.toJson(componentInfo.get("yAxisCol")), new TypeReference<List<Map<String, Object>>>() {
        });
        String[] y_columns = new String[yAxisColList.size()];
        for (int i = 0; i < yAxisColList.size(); i++) {
            y_columns[i] = yAxisColList.get(i).get("column_name").toString();
        }
        Map<String, Object> visualComponentResult = getVisualComponentResult(auto_comp_sum.getExe_sql(), 50);
        componentInfo.putAll(visualComponentResult);
        Map<String, Object> chartShowMap = getChartShow(auto_comp_sum.getExe_sql(), x_columns, y_columns, auto_comp_sum.getChart_type(), 50);
        componentInfo.putAll(chartShowMap);
        Map<String, Object> tableColumn = getColumnByName(auto_comp_sum.getSources_obj(), auto_comp_sum.getData_source());
        componentInfo.put("columnAndNumberColumnInfo", tableColumn);
        return componentInfo;
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
    public Long updateVisualComponentInfo(String componentBeanString, String auto_comp_sumString, String autoCompCondString, String autoCompGroupString, String autoCompDataSumString, String titleFontString, String axisStyleFontString, String autoAxisInfoString, String xAxisLabelString, String yAxisLabelString, String xAxisLineString, String yAxisLineString, String auto_table_infoString, String auto_chartsconfigString, String auto_labelString, String auto_legend_infoString, String allcolumn) {
        ComponentBean componentBean = JsonUtil.toObject(componentBeanString, new TypeReference<ComponentBean>() {
        });
        AutoCompSum auto_comp_sum = JsonUtil.toObject(auto_comp_sumString, new TypeReference<AutoCompSum>() {
        });
        AutoFontInfo titleFont = JsonUtil.toObject(titleFontString, new TypeReference<AutoFontInfo>() {
        });
        AutoFontInfo axisStyleFont = JsonUtil.toObject(axisStyleFontString, new TypeReference<AutoFontInfo>() {
        });
        AutoAxislabelInfo xAxisLabel = JsonUtil.toObject(xAxisLabelString, new TypeReference<AutoAxislabelInfo>() {
        });
        AutoAxislabelInfo yAxisLabel = JsonUtil.toObject(yAxisLabelString, new TypeReference<AutoAxislabelInfo>() {
        });
        AutoAxislineInfo xAxisLine = JsonUtil.toObject(xAxisLineString, new TypeReference<AutoAxislineInfo>() {
        });
        AutoAxislineInfo yAxisLine = JsonUtil.toObject(yAxisLineString, new TypeReference<AutoAxislineInfo>() {
        });
        AutoTableInfo auto_table_info = JsonUtil.toObject(auto_table_infoString, new TypeReference<AutoTableInfo>() {
        });
        AutoChartsconfig auto_chartsconfig = JsonUtil.toObject(auto_chartsconfigString, new TypeReference<AutoChartsconfig>() {
        });
        AutoLabel auto_label = JsonUtil.toObject(auto_labelString, new TypeReference<AutoLabel>() {
        });
        AutoLegendInfo auto_legend_info = JsonUtil.toObject(auto_legend_infoString, new TypeReference<AutoLegendInfo>() {
        });
        AutoCompCond[] autoCompConds = JsonUtil.toObject(autoCompCondString, new TypeReference<AutoCompCond[]>() {
        });
        AutoCompGroup[] autoCompGroups = JsonUtil.toObject(autoCompGroupString, new TypeReference<AutoCompGroup[]>() {
        });
        AutoCompDataSum[] autoCompDataSums = JsonUtil.toObject(autoCompDataSumString, new TypeReference<AutoCompDataSum[]>() {
        });
        AutoAxisInfo[] autoAxisInfos = JsonUtil.toObject(autoAxisInfoString, new TypeReference<AutoAxisInfo[]>() {
        });
        Validator.notNull(auto_comp_sum.getComponent_id(), "更新时组件ID不能为空");
        checkAutoCompSumFields(auto_comp_sum);
        auto_comp_sum.setLast_update_date(DateUtil.getSysDate());
        auto_comp_sum.setLast_update_time(DateUtil.getSysTime());
        auto_comp_sum.setUpdate_user(UserUtil.getUserId());
        String exe_sql = getSqlByCondition(componentBean, autoCompConds, autoCompGroups, autoCompDataSums);
        auto_comp_sum.setExe_sql(exe_sql);
        Map<String, Object> chartShow = getChartShow(exe_sql, componentBean.getX_columns(), componentBean.getY_columns(), auto_comp_sum.getChart_type(), 50);
        chartShow.put("itemStyle", auto_label);
        auto_comp_sum.setComponent_buffer(JsonUtil.toJson(chartShow));
        auto_comp_sum.update(Dbo.db());
        Validator.notBlank(componentBean.getFetch_name(), "取数名称不能为空");
        deleteComponentAssociateTable(auto_comp_sum.getComponent_id());
        saveComponentInfo(componentBean, auto_comp_sum, titleFont, axisStyleFont, xAxisLabel, yAxisLabel, xAxisLine, yAxisLine, auto_chartsconfig, auto_label, auto_legend_info, autoCompConds, autoCompGroups, autoCompDataSums, autoAxisInfos, auto_table_info);
        return auto_comp_sum.getComponent_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_comp_sum", desc = "", range = "", isBean = true)
    private void checkAutoCompSumFields(AutoCompSum auto_comp_sum) {
        Validator.notBlank(auto_comp_sum.getChart_type(), "图标类型不能为空");
        Validator.notBlank(auto_comp_sum.getComponent_name(), "组件名称不能为空");
        Validator.notBlank(auto_comp_sum.getData_source(), "数据来源不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "component_name", desc = "", range = "")
    private void isAutoCompSumExist(String component_name) {
        if (Dbo.queryNumber("SELECT count(1) FROM " + AutoCompSum.TableName + " WHERE component_name = ?", component_name).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("组件名称已存在");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "exe_sql", desc = "", range = "")
    @Param(name = "x_columns", desc = "", range = "", nullable = true)
    @Param(name = "y_columns", desc = "", range = "", nullable = true)
    @Param(name = "chart_type", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "50")
    @Return(desc = "", range = "")
    public Map<String, Object> getChartShow(String exe_sql, String[] x_columns, String[] y_columns, String chart_type, Integer showNum) {
        List<Map<String, Object>> componentList = new ArrayList<>();
        Set<String> columns = new HashSet<>();
        if (showNum > 1000) {
            showNum = 1000;
        }
        new ProcessingData() {

            @Override
            public void dealLine(Map<String, Object> map) {
                map.forEach((k, v) -> columns.add(k));
                componentList.add(map);
            }
        }.getPageDataLayer(exe_sql, Dbo.db(), 1, showNum <= 0 ? 50 : showNum);
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("chart_type", chart_type);
        logger.info("----------------------chart_type:" + chart_type);
        if (LINE.equals(chart_type) || BAR.equals(chart_type) || BL.equals(chart_type)) {
            putDataForLine(componentList, x_columns, y_columns, chart_type, resultMap);
        } else if (BLSIMPLE.equals(chart_type)) {
            putDataForBLSimple(componentList, x_columns, y_columns, resultMap);
        } else if (STACKINGBAR.equals(chart_type)) {
            putDataForStackingBar(componentList, x_columns, y_columns, resultMap);
        } else if (POLARBAR.equals(chart_type)) {
            putDataForPolarbar(componentList, x_columns, y_columns, resultMap);
        } else if (PIE.equals(chart_type) || HUANPIE.equals(chart_type) || FASANPIE.equals(chart_type)) {
            putDataForPie(componentList, x_columns, y_columns, chart_type, resultMap);
        } else if (SCATTER.equals(chart_type)) {
            putDataForScatter(componentList, x_columns, y_columns, resultMap);
        } else if (CARD.equals(chart_type)) {
            List<Object> cardData = new ArrayList<>();
            componentList.get(0).forEach((k, v) -> cardData.add(v));
            resultMap.put("cardData", cardData);
        } else if (TABLE.equals(chart_type)) {
            resultMap.put("tableData", componentList);
            resultMap.put("columns", columns);
        } else if (TREEMAP.equals(chart_type)) {
            putDataForTreemap(componentList, x_columns, y_columns, resultMap);
        } else if (MAP.equals(chart_type) || BUBBLE.equals(chart_type)) {
            putDataForBubbleOrMap(componentList, x_columns, y_columns, resultMap);
        } else {
            throw new BusinessException("暂不支持该种图例类型" + chart_type);
        }
        return resultMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getDataDashboardInfo(int currPage, int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> dashboardInfoList = Dbo.queryPagedList(page, "SELECT *,case when dashboard_theme = ? then true else false end as iflargescreen " + "  FROM " + AutoDashboardInfo.TableName + " WHERE user_id = ? order by create_date desc,create_time desc", LargeScreenTheme, UserUtil.getUserId());
        Map<String, Object> dashboardInfoMap = new HashMap<>();
        dashboardInfoMap.put("dashboardInfoList", dashboardInfoList);
        dashboardInfoMap.put("totalSize", page.getTotalSize());
        return dashboardInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dashboardName", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getDataDashboardInfoByName(String dashboardName, Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> dashboardInfoList = Dbo.queryPagedList(page, "SELECT *,case when dashboard_theme = ? then true else false end as iflargescreen " + "  FROM " + AutoDashboardInfo.TableName + " WHERE user_id = ? and dashboard_name like ? order by create_date desc,create_time desc", LargeScreenTheme, UserUtil.getUserId(), "%" + dashboardName + "%");
        Map<String, Object> dashboardInfoMap = new HashMap<>();
        dashboardInfoMap.put("dashboardInfoList", dashboardInfoList);
        dashboardInfoMap.put("totalSize", page.getTotalSize());
        return dashboardInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoCompSums", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> showComponentOnDashboard(String autoCompSums) {
        List<AutoCompSum> autoCompSumList = JsonUtil.toObject(autoCompSums, new TypeReference<List<AutoCompSum>>() {
        });
        Map<String, Object> componentOnDashBoard = new HashMap<>();
        if (!autoCompSumList.isEmpty()) {
            List<Map<String, Object>> componentList = new ArrayList<>();
            for (int i = 0; i < autoCompSumList.size(); i++) {
                AutoCompSum autoCompSum = autoCompSumList.get(i);
                Map<String, Object> componentInfo = getVisualComponentInfoById(autoCompSum.getComponent_id());
                AutoCompSum auto_comp_sum = JsonUtil.toObjectSafety(JsonUtil.toJson(componentInfo.get("compSum")), AutoCompSum.class).orElseThrow(() -> new BusinessException("转换实体失败"));
                componentInfo.put("x", (i % 2) * (48 + (i % 2)));
                componentInfo.put("y", (i / 2) * (27 + i / 2));
                componentInfo.put("w", 48);
                componentInfo.put("h", 27);
                componentInfo.put("i", autoCompSum.getComponent_id());
                componentInfo.put("static", false);
                componentInfo.put("type", String.valueOf(autoCompSum.getComponent_id()));
                componentOnDashBoard.put(String.valueOf(autoCompSum.getComponent_id()), auto_comp_sum.getComponent_buffer());
                componentList.add(componentInfo);
            }
            componentOnDashBoard.put("layout", componentList);
        }
        return componentOnDashBoard;
    }

    public Map<Long, Object> showComponentData(List<Long> component_ids) {
        Map<Long, Object> resultMap = new HashMap<>();
        if (component_ids != null && component_ids.size() > 0) {
            for (int i = 0; i < component_ids.size(); i++) {
                Map<String, Object> componentInfo = AutoAnalysisUtil.getVisualComponentInfoById(component_ids.get(i), Dbo.db());
                AutoCompSum auto_comp_sum = JsonUtil.toObjectSafety(JsonUtil.toJson(componentInfo.get("compSum")), AutoCompSum.class).orElseThrow(() -> new BusinessException("实体" + AutoCompSum.TableName + "转换失败"));
                List<Map<String, Object>> xAxisColList = JsonUtil.toObject(JsonUtil.toJson(componentInfo.get("xAxisCol")), new TypeReference<List<Map<String, Object>>>() {
                });
                String[] x_columns = new String[xAxisColList.size()];
                for (int j = 0; j < xAxisColList.size(); j++) {
                    x_columns[j] = xAxisColList.get(j).get("column_name").toString();
                }
                List<Map<String, Object>> yAxisColList = JsonUtil.toObject(JsonUtil.toJson(componentInfo.get("yAxisCol")), new TypeReference<List<Map<String, Object>>>() {
                });
                String[] y_columns = new String[yAxisColList.size()];
                for (int k = 0; k < yAxisColList.size(); k++) {
                    y_columns[k] = yAxisColList.get(k).get("column_name").toString();
                }
                Map<String, Object> chartShowMap = getChartShow(auto_comp_sum.getExe_sql(), x_columns, y_columns, auto_comp_sum.getChart_type(), 50);
                chartShowMap.put("chart_theme", auto_comp_sum.getComponent_name());
                chartShowMap.put("chart_title", auto_comp_sum.getChart_theme());
                resultMap.put(component_ids.get(i), chartShowMap);
            }
        }
        return resultMap;
    }

    private void putDataForBubbleOrMap(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, Map<String, Object> resultMap) {
        List<Map<String, Object>> seriesData = new ArrayList<>();
        for (Map<String, Object> stringObjectMap : componentList) {
            Map<String, Object> map = new HashMap<>();
            map.put("name", stringObjectMap.get(x_columns[0]));
            map.put("value", stringObjectMap.get(y_columns[0]));
            seriesData.add(map);
        }
        resultMap.put("seriesData", seriesData);
    }

    private void putDataForTreemap(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, Map<String, Object> resultMap) {
        List<Map<String, Object>> seriesData = new ArrayList<>();
        Map<String, Map<String, Object>> map = new HashMap<>();
        for (Map<String, Object> stringObjectMap : componentList) {
            Map<String, Object> xMap = new HashMap<>();
            if (x_columns.length == 1) {
                xMap.put("name", stringObjectMap.get(x_columns[0]));
                xMap.put("value", stringObjectMap.get(y_columns[0]));
                seriesData.add(xMap);
            } else {
                String childrenName = stringObjectMap.get(x_columns[0]).toString();
                xMap.put("name", stringObjectMap.get(x_columns[1]));
                xMap.put("value", stringObjectMap.get(y_columns[0]));
                Map<String, Object> mapTemp;
                if (!map.containsKey(childrenName)) {
                    mapTemp = new HashMap<>();
                    List<Object> list = new ArrayList<>();
                    list.add(map);
                    mapTemp.put("children", list);
                    mapTemp.put("name", childrenName);
                } else {
                    mapTemp = map.get(childrenName);
                    List<Object> mapList = JsonUtil.toObject(JsonUtil.toJson(mapTemp.get("children")), new TypeReference<List<Object>>() {
                    });
                    mapList.add(map);
                    mapTemp.put("children", mapList);
                }
                map.put(childrenName, mapTemp);
            }
        }
        if (x_columns.length == 1) {
            resultMap.put("seriesData", seriesData);
        } else {
            resultMap.put("seriesData", map.values());
        }
    }

    private void putDataForScatter(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, Map<String, Object> resultMap) {
        List<Object> scatterData = new ArrayList<>();
        for (Map<String, Object> stringObjectMap : componentList) {
            List<Object> list = new ArrayList<>();
            String x = stringObjectMap.get(x_columns[0].toLowerCase()) == null ? stringObjectMap.get(x_columns[0].toUpperCase()).toString() : stringObjectMap.get(x_columns[0].toLowerCase()).toString();
            String y = stringObjectMap.get(y_columns[0].toLowerCase()) == null ? stringObjectMap.get(y_columns[0].toUpperCase()).toString() : stringObjectMap.get(y_columns[0].toLowerCase()).toString();
            x = checkIfNumeric(x, x_columns[0]);
            y = checkIfNumeric(y, y_columns[0]);
            if (!x.toLowerCase().equals("null") && !x.trim().equals("") && !y.toLowerCase().equals("null") && !y.trim().equals("")) {
                list.add(x);
                list.add(y);
                scatterData.add(list);
            }
        }
        resultMap.put("legend_data", y_columns);
        resultMap.put("scatterData", scatterData);
    }

    private void putDataForPie(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, String chart_type, Map<String, Object> resultMap) {
        List<String> legendData = new ArrayList<>();
        List<Map<String, Object>> seriesArray = new ArrayList<>();
        List<Map<String, Object>> seriesData = new ArrayList<>();
        BigDecimal count = new BigDecimal(0);
        for (Map<String, Object> stringObjectMap : componentList) {
            Map<String, Object> map = new HashMap<>();
            legendData.add(stringObjectMap.get(x_columns[0].toLowerCase()) == null ? stringObjectMap.get(x_columns[0].toUpperCase()).toString() : stringObjectMap.get(x_columns[0].toLowerCase()).toString());
            map.put("name", stringObjectMap.get(x_columns[0]));
            map.put("value", stringObjectMap.get(y_columns[0]));
            String s = stringObjectMap.get(y_columns[0].toLowerCase()) == null ? stringObjectMap.get(y_columns[0].toUpperCase()).toString() : stringObjectMap.get(y_columns[0].toLowerCase()).toString();
            s = checkIfNumeric(s, y_columns[0]);
            if (!s.toLowerCase().equals("null") && !s.trim().equals("")) {
                count = count.add(new BigDecimal(s));
            }
            seriesData.add(map);
        }
        resultMap.put("count", count);
        Map<String, Object> series = new HashMap<>();
        series.put("data", seriesData);
        series.put("name", x_columns[0]);
        series.put("type", "pie");
        if (PIE.equals(chart_type)) {
            series.put("radius", "80%");
            resultMap.put("legendData", legendData);
        } else if (HUANPIE.equals(chart_type)) {
            List<String> radius = new ArrayList<>();
            radius.add("35%");
            radius.add("80%");
            series.put("radius", radius);
            resultMap.put("pietype", "huanpie");
            resultMap.put("legendData", legendData);
        } else if (FASANPIE.equals(chart_type)) {
            series.put("radius", "80%");
            series.put("roseType", "radius");
            resultMap.put("legendData", legendData);
        }
        seriesArray.add(series);
        resultMap.put("seriesArray", seriesArray);
    }

    private void putDataForPolarbar(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, Map<String, Object> resultMap) {
        if (y_columns != null && y_columns.length > 0) {
            resultMap.put("legend_data", y_columns);
            List<Object> yList = new ArrayList<>();
            for (String y_column : y_columns) {
                Map<String, Object> map = new HashMap<>();
                List<Object> data = new ArrayList<>();
                for (Map<String, Object> stringObjectMap : componentList) {
                    String s = stringObjectMap.get(y_column.trim().toLowerCase()) == null ? stringObjectMap.get(y_column.trim().toUpperCase()).toString().trim() : stringObjectMap.get(y_column.trim().toLowerCase()).toString().trim();
                    s = checkIfNumeric(s, y_column);
                    if (!s.toLowerCase().equals("null") && !s.trim().equals("")) {
                        data.add(s);
                    }
                }
                map.put("name", y_column);
                map.put("type", "bar");
                map.put("data", data);
                map.put("coordinateSystem", "polar");
                yList.add(map);
            }
            resultMap.put("seriesArray", yList);
        }
        if (x_columns != null && x_columns.length > 0) {
            List<String> xList = new ArrayList<>();
            for (Map<String, Object> stringObjectMap : componentList) {
                xList.add(stringObjectMap.get(x_columns[0].trim().toLowerCase()) == null ? stringObjectMap.get(x_columns[0].trim().toUpperCase()).toString() : stringObjectMap.get(x_columns[0].trim().toLowerCase()).toString());
            }
            resultMap.put("xArray", xList);
        }
    }

    private void putDataForStackingBar(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, Map<String, Object> resultMap) {
        if (y_columns != null && y_columns.length > 0) {
            resultMap.put("legend_data", y_columns);
            List<Object> yList = new ArrayList<>();
            Map<String, Object> labelMap = new HashMap<>();
            labelMap.put("show", true);
            labelMap.put("position", "inside");
            for (String y_column : y_columns) {
                Map<String, Object> map = new HashMap<>();
                List<Object> data = new ArrayList<>();
                for (Map<String, Object> stringObjectMap : componentList) {
                    String s = stringObjectMap.get(y_column.trim().toLowerCase()) == null ? stringObjectMap.get(y_column.trim().toUpperCase()).toString() : stringObjectMap.get(y_column.trim().toLowerCase()).toString();
                    s = checkIfNumeric(s, y_column);
                    if (!s.toLowerCase().equals("null") && !s.trim().equals("")) {
                        data.add(s);
                    }
                }
                map.put("name", y_column);
                map.put("type", "bar");
                map.put("stack", "总量");
                map.put("label", labelMap);
                map.put("data", data);
                yList.add(map);
            }
            resultMap.put("seriesArray", yList);
        }
        if (x_columns != null && x_columns.length > 0) {
            List<String> xList = new ArrayList<>();
            for (Map<String, Object> stringObjectMap : componentList) {
                xList.add(stringObjectMap.get(x_columns[0].trim().toLowerCase()) == null ? stringObjectMap.get(x_columns[0].trim().toUpperCase()).toString() : stringObjectMap.get(x_columns[0].trim().toLowerCase()).toString());
            }
            resultMap.put("xArray", xList);
        }
    }

    private void putDataForBLSimple(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, Map<String, Object> resultMap) {
        if (x_columns.length < 1 || y_columns.length < 2) {
            return;
        }
        List<Object> xAxisData = new ArrayList<>();
        List<Object> yAxisData1 = new ArrayList<>();
        List<Object> yAxisData2 = new ArrayList<>();
        for (Map<String, Object> stringObjectMap : componentList) {
            xAxisData.add(stringObjectMap.get(x_columns[0]));
            yAxisData1.add(stringObjectMap.get(y_columns[0]));
            yAxisData2.add(stringObjectMap.get(y_columns[1]));
        }
        resultMap.put("series1Name", y_columns[0]);
        resultMap.put("series1Data", yAxisData1);
        resultMap.put("series2Name", y_columns[1]);
        resultMap.put("series2Data", yAxisData2);
        resultMap.put("xAxisData", xAxisData);
    }

    private void putDataForLine(List<Map<String, Object>> componentList, String[] x_columns, String[] y_columns, String chart_type, Map<String, Object> resultMap) {
        if (y_columns != null && y_columns.length > 0) {
            resultMap.put("legend_data", y_columns);
            List<Object> yList = new ArrayList<>();
            for (int j = 0; j < y_columns.length; j++) {
                Map<String, Object> map = new HashMap<>();
                List<Object> data = new ArrayList<>();
                for (Map<String, Object> stringObjectMap : componentList) {
                    String s = stringObjectMap.get(y_columns[j].trim().toLowerCase()) == null ? stringObjectMap.get(y_columns[j].trim().toUpperCase()).toString() : stringObjectMap.get(y_columns[j].trim().toLowerCase()).toString();
                    s = checkIfNumeric(s, y_columns[j].trim());
                    if (!s.toLowerCase().equals("null") && !s.trim().equals("")) {
                        data.add(s);
                    }
                }
                map.put("data", data);
                map.put("name", y_columns[j].trim());
                if (BL.equals(chart_type)) {
                    if (j < 2) {
                        map.put("type", BAR);
                        map.put("stack", "two");
                    } else {
                        map.put("type", LINE);
                        map.put("yAxisIndex", j - 2);
                    }
                } else {
                    map.put("type", chart_type);
                }
                yList.add(map);
            }
            resultMap.put("seriesArray", yList);
        }
        if (x_columns != null && x_columns.length > 0) {
            List<String> xList = new ArrayList<>();
            for (Map<String, Object> stringObjectMap : componentList) {
                xList.add(stringObjectMap.get(x_columns[0].trim().toLowerCase()) == null ? stringObjectMap.get(x_columns[0].trim().trim().toUpperCase()).toString() : stringObjectMap.get(x_columns[0].trim().trim().toLowerCase()).toString());
            }
            resultMap.put("xArray", xList);
        }
    }

    private String checkIfNumeric(String s, String columnName) {
        if (s == null || s.toLowerCase().equals("null") || s.trim().equals("")) {
            logger.info(columnName + "字段包含空值");
            return "";
        }
        s = s.trim();
        if (!s.matches("-[0-9]+(.[0-9]+)?|[0-9]+(.[0-9]+)?")) {
            throw new BusinessException(columnName + "字段包含不是数值的值，无法构成图");
        } else {
            return s;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "componentBean", desc = "", range = "")
    @Param(name = "auto_comp_sum", desc = "", range = "")
    @Param(name = "autoCompConds", desc = "", range = "", nullable = true)
    @Param(name = "autoCompGroups", desc = "", range = "", nullable = true)
    @Param(name = "autoCompDataSums", desc = "", range = "")
    @Param(name = "titleFont", desc = "", range = "")
    @Param(name = "axisStyleFont", desc = "", range = "")
    @Param(name = "autoAxisInfo", desc = "", range = "")
    @Param(name = "xAxisLabel", desc = "", range = "")
    @Param(name = "yAxisLabel", desc = "", range = "")
    @Param(name = "xAxisLine", desc = "", range = "")
    @Param(name = "yAxisLine", desc = "", range = "")
    @Param(name = "auto_table_info", desc = "", range = "")
    @Param(name = "auto_chartsconfig", desc = "", range = "")
    @Param(name = "auto_label", desc = "", range = "")
    @Param(name = "auto_legend_info", desc = "", range = "")
    @Param(name = "auto_table_info", desc = "", range = "")
    private void saveComponentInfo(ComponentBean componentBean, AutoCompSum auto_comp_sum, AutoFontInfo titleFont, AutoFontInfo axisStyleFont, AutoAxislabelInfo xAxisLabel, AutoAxislabelInfo yAxisLabel, AutoAxislineInfo xAxisLine, AutoAxislineInfo yAxisLine, AutoChartsconfig auto_chartsconfig, AutoLabel auto_label, AutoLegendInfo auto_legend_info, AutoCompCond[] autoCompConds, AutoCompGroup[] autoCompGroups, AutoCompDataSum[] autoCompDataSums, AutoAxisInfo[] autoAxisInfos, AutoTableInfo auto_table_info) {
        if (autoCompConds != null && autoCompConds.length > 0) {
            for (AutoCompCond auto_comp_cond : autoCompConds) {
                Validator.notBlank(auto_comp_cond.getCond_en_column(), "条件英文字段不能为空");
                Validator.notBlank(auto_comp_cond.getOperator(), "操作符不能为空");
                auto_comp_cond.setCreate_date(DateUtil.getSysDate());
                auto_comp_cond.setCreate_time(DateUtil.getSysTime());
                auto_comp_cond.setCreate_user(UserUtil.getUserId());
                auto_comp_cond.setLast_update_date(DateUtil.getSysDate());
                auto_comp_cond.setLast_update_time(DateUtil.getSysTime());
                auto_comp_cond.setUpdate_user(UserUtil.getUserId());
                auto_comp_cond.setComponent_id(auto_comp_sum.getComponent_id());
                auto_comp_cond.setComponent_cond_id(PrimayKeyGener.getNextId());
                auto_comp_cond.add(Dbo.db());
            }
        }
        if (autoCompGroups != null && autoCompGroups.length > 0) {
            for (AutoCompGroup auto_comp_group : autoCompGroups) {
                Validator.notBlank(auto_comp_group.getColumn_name(), "字段名不能为空");
                auto_comp_group.setCreate_date(DateUtil.getSysDate());
                auto_comp_group.setCreate_time(DateUtil.getSysTime());
                auto_comp_group.setCreate_user(UserUtil.getUserId());
                auto_comp_group.setLast_update_date(DateUtil.getSysDate());
                auto_comp_group.setLast_update_time(DateUtil.getSysTime());
                auto_comp_group.setUpdate_user(UserUtil.getUserId());
                auto_comp_group.setComponent_id(auto_comp_sum.getComponent_id());
                auto_comp_group.setComponent_group_id(PrimayKeyGener.getNextId());
                auto_comp_group.add(Dbo.db());
            }
        }
        if (autoCompDataSums != null && autoCompDataSums.length > 0) {
            for (AutoCompDataSum auto_comp_data_sum : autoCompDataSums) {
                Validator.notBlank(auto_comp_data_sum.getColumn_name(), "字段名不能为空");
                Validator.notBlank(auto_comp_data_sum.getSummary_type(), "汇总类型不能为空");
                auto_comp_data_sum.setCreate_date(DateUtil.getSysDate());
                auto_comp_data_sum.setCreate_time(DateUtil.getSysTime());
                auto_comp_data_sum.setCreate_user(UserUtil.getUserId());
                auto_comp_data_sum.setLast_update_date(DateUtil.getSysDate());
                auto_comp_data_sum.setLast_update_time(DateUtil.getSysTime());
                auto_comp_data_sum.setUpdate_user(UserUtil.getUserId());
                auto_comp_data_sum.setComponent_id(auto_comp_sum.getComponent_id());
                auto_comp_data_sum.setComp_data_sum_id(PrimayKeyGener.getNextId());
                auto_comp_data_sum.add(Dbo.db());
            }
        }
        addAutoAxisColInfo(componentBean, auto_comp_sum);
        if (titleFont != null) {
            titleFont.setFont_id(PrimayKeyGener.getNextId());
            titleFont.setFont_corr_id(auto_comp_sum.getComponent_id());
            titleFont.setFont_corr_tname(AutoCompSum.TableName);
            titleFont.add(Dbo.db());
        }
        if (autoAxisInfos != null) {
            for (AutoAxisInfo auto_axis_info : autoAxisInfos) {
                Validator.notBlank(auto_axis_info.getAxis_type(), "轴类型不能为空");
                auto_axis_info.setAxis_id(PrimayKeyGener.getNextId());
                auto_axis_info.setComponent_id(auto_comp_sum.getComponent_id());
                auto_axis_info.add(Dbo.db());
                axisStyleFont.setFont_id(PrimayKeyGener.getNextId());
                axisStyleFont.setFont_corr_id(auto_axis_info.getAxis_id());
                axisStyleFont.setFont_corr_tname(AutoAxisInfo.TableName);
                axisStyleFont.add(Dbo.db());
                if (AxisType.XAxis == AxisType.ofEnumByCode(auto_axis_info.getAxis_type())) {
                    xAxisLine.setAxis_id(auto_axis_info.getAxis_id());
                    xAxisLine.setAxisline_id(PrimayKeyGener.getNextId());
                    xAxisLine.add(Dbo.db());
                    xAxisLabel.setLable_id(PrimayKeyGener.getNextId());
                    xAxisLabel.setAxis_id(auto_axis_info.getAxis_id());
                    xAxisLabel.add(Dbo.db());
                } else {
                    yAxisLine.setAxis_id(auto_axis_info.getAxis_id());
                    yAxisLine.setAxisline_id(PrimayKeyGener.getNextId());
                    yAxisLine.add(Dbo.db());
                    yAxisLabel.setLable_id(PrimayKeyGener.getNextId());
                    yAxisLabel.setAxis_id(auto_axis_info.getAxis_id());
                    yAxisLabel.add(Dbo.db());
                }
            }
        }
        if (null != auto_table_info) {
            auto_table_info.setConfig_id(PrimayKeyGener.getNextId());
            auto_table_info.setComponent_id(auto_comp_sum.getComponent_id());
            auto_table_info.add(Dbo.db());
        }
        if (auto_chartsconfig != null) {
            auto_chartsconfig.setConfig_id(PrimayKeyGener.getNextId());
            auto_chartsconfig.setComponent_id(auto_comp_sum.getComponent_id());
            auto_chartsconfig.setShowsymbol(auto_chartsconfig.getSymbol() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getSymbol());
            auto_chartsconfig.setConnectnulls(auto_chartsconfig.getConnectnulls() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getConnectnulls());
            auto_chartsconfig.setStep(auto_chartsconfig.getStep() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getStep());
            auto_chartsconfig.setSmooth(auto_chartsconfig.getSmooth() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getSmooth());
            auto_chartsconfig.setSilent(auto_chartsconfig.getSilent() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getSilent());
            auto_chartsconfig.setLegendhoverlink(auto_chartsconfig.getLegendhoverlink() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getLegendhoverlink());
            auto_chartsconfig.setClockwise(auto_chartsconfig.getClockwise() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getClockwise());
            auto_chartsconfig.setRosetype(auto_chartsconfig.getRosetype() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getRosetype());
            auto_chartsconfig.setPolyline(auto_chartsconfig.getPolyline() == null ? IsFlag.Fou.getCode() : auto_chartsconfig.getPolyline());
            auto_chartsconfig.add(Dbo.db());
        }
        if (auto_label != null) {
            auto_label.setLable_id(PrimayKeyGener.getNextId());
            auto_label.setLabel_corr_tname(AutoChartsconfig.TableName);
            auto_label.setLabel_corr_id(auto_comp_sum.getComponent_id());
            auto_label.add(Dbo.db());
        }
        if (null != auto_legend_info) {
            auto_legend_info.setLegend_id(PrimayKeyGener.getNextId());
            auto_legend_info.setComponent_id(auto_comp_sum.getComponent_id());
            auto_legend_info.add(Dbo.db());
        }
    }

    private void addAutoAxisColInfo(ComponentBean componentBean, AutoCompSum auto_comp_sum) {
        String[] x_columns = componentBean.getX_columns();
        if (x_columns != null && x_columns.length > 0) {
            for (int i = 0; i < x_columns.length; i++) {
                AutoAxisColInfo auto_axis_col_info = new AutoAxisColInfo();
                auto_axis_col_info.setAxis_column_id(PrimayKeyGener.getNextId());
                auto_axis_col_info.setSerial_number(i);
                auto_axis_col_info.setColumn_name(x_columns[i]);
                auto_axis_col_info.setShow_type(AxisType.XAxis.getCode());
                auto_axis_col_info.setComponent_id(auto_comp_sum.getComponent_id());
                auto_axis_col_info.add(Dbo.db());
            }
        }
        String[] y_columns = componentBean.getY_columns();
        if (y_columns != null && y_columns.length > 0) {
            for (int i = 0; i < y_columns.length; i++) {
                AutoAxisColInfo auto_axis_col_info = new AutoAxisColInfo();
                auto_axis_col_info.setAxis_column_id(PrimayKeyGener.getNextId());
                auto_axis_col_info.setSerial_number(i);
                auto_axis_col_info.setColumn_name(y_columns[i]);
                auto_axis_col_info.setShow_type(AxisType.YAxis.getCode());
                auto_axis_col_info.setComponent_id(auto_comp_sum.getComponent_id());
                auto_axis_col_info.add(Dbo.db());
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public String getAccessSql(long fetch_sum_id) {
        AutoFetchSum auto_fetch_sum = Dbo.queryOneObject(AutoFetchSum.class, "select fetch_sql from " + AutoFetchSum.TableName + " where fetch_sum_id = ?", fetch_sum_id).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
        return auto_fetch_sum.getFetch_sql();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "layerBean", desc = "", range = "")
    @Return(desc = "", range = "")
    private String getDatabaseType(LayerBean layerBean) {
        String store_type = layerBean.getStore_type();
        if (store_type.equals(Store_type.DATABASE.getCode())) {
            Map<String, String> layerAttr = layerBean.getLayerAttr();
            String database_type = layerAttr.get("database_type");
            if (!StringUtil.isEmpty(database_type)) {
                return database_type;
            } else {
                logger.error("根据存储层信息未找到存储数据库，layerBean：" + layerBean);
                throw new BusinessException("根据存储层信息未找到存储数据库");
            }
        } else {
            return Store_type.ofValueByCode(store_type).toLowerCase();
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_comp_data_sum", desc = "", range = "", isBean = true)
    @Return(desc = "", range = "")
    private String getSelectSql(AutoCompDataSum auto_comp_data_sum, String seperator, List<String> allcolumnlist) {
        String column_name = auto_comp_data_sum.getColumn_name();
        String summary_type = auto_comp_data_sum.getSummary_type();
        if (AutoDataSumType.QiuHe == AutoDataSumType.ofEnumByCode(summary_type)) {
            return "sum(" + column_name + ") as " + seperator + "sum_" + column_name + seperator + " ,";
        } else if (AutoDataSumType.QiuPingJun == AutoDataSumType.ofEnumByCode(summary_type)) {
            return "avg(" + column_name + ") as " + seperator + "avg_" + column_name + seperator + " ,";
        } else if (AutoDataSumType.QiuZuiDaZhi == AutoDataSumType.ofEnumByCode(summary_type)) {
            return "max(" + column_name + ") as " + seperator + "max_" + column_name + seperator + " ,";
        } else if (AutoDataSumType.QiuZuiXiaoZhi == AutoDataSumType.ofEnumByCode(summary_type)) {
            return "min(" + column_name + ") as " + seperator + "min_" + column_name + seperator + " ,";
        } else if (AutoDataSumType.ZongHangShu == AutoDataSumType.ofEnumByCode(summary_type)) {
            return column_name + " as " + seperator + "count" + seperator + " ,";
        } else if (AutoDataSumType.YuanShiShuJu == AutoDataSumType.ofEnumByCode(summary_type)) {
            return column_name + " as " + seperator + column_name + seperator + ",";
        } else if (AutoDataSumType.ChaKanQuanBu == AutoDataSumType.ofEnumByCode(summary_type)) {
            StringBuilder result = new StringBuilder();
            for (String column : allcolumnlist) {
                result.append(column).append(" as ").append(seperator).append(column).append(seperator).append(",");
            }
            return result.toString();
        } else {
            throw new BusinessException("当前查询内容不存在于代码项中:" + summary_type);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_comp_cond", desc = "", range = "", isBean = true)
    @Param(name = "upAndLowArray", desc = "", range = "")
    @Return(desc = "", range = "")
    private String getCondSql(AutoCompCond auto_comp_cond, ArrayList<Map<String, String>> upAndLowArray) {
        String cond_en_column = auto_comp_cond.getCond_en_column();
        String operator = auto_comp_cond.getOperator();
        String cond_value = auto_comp_cond.getCond_value();
        if (AutoDataOperator.JieYu == AutoDataOperator.ofEnumByCode(operator)) {
            String[] split = cond_value.split(",");
            if (split.length != 2) {
                throw new BusinessException("处理" + AutoDataOperator.JieYu.getValue() + "方法出错，参数个数错误");
            }
            return cond_en_column + Constant.SPACE + "BETWEEN" + Constant.SPACE + split[0] + Constant.SPACE + "AND" + Constant.SPACE + split[1] + Constant.SPACE;
        } else if (AutoDataOperator.BuJieYu == AutoDataOperator.ofEnumByCode(operator)) {
            String[] split = cond_value.split(",");
            if (split.length != 2) {
                throw new BusinessException("处理" + AutoDataOperator.BuJieYu.getValue() + "方法出错，参数个数错误");
            }
            return cond_en_column + Constant.SPACE + "NOT BETWEEN" + Constant.SPACE + split[0] + Constant.SPACE + "AND" + Constant.SPACE + split[1] + Constant.SPACE;
        } else if (AutoDataOperator.DengYu == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + "=" + Constant.SPACE + cond_value + Constant.SPACE;
        } else if (AutoDataOperator.BuDengYu == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + "!=" + Constant.SPACE + cond_value + Constant.SPACE;
        } else if (AutoDataOperator.DaYu == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + ">" + Constant.SPACE + cond_value + Constant.SPACE;
        } else if (AutoDataOperator.XiaoYu == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + "<" + Constant.SPACE + cond_value + Constant.SPACE;
        } else if (AutoDataOperator.DaYuDengYu == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + ">=" + Constant.SPACE + cond_value + Constant.SPACE;
        } else if (AutoDataOperator.XiaoYuDengYu == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + "<=" + Constant.SPACE + cond_value + Constant.SPACE;
        } else if (AutoDataOperator.ZuiDaDeNGe == AutoDataOperator.ofEnumByCode(operator)) {
            Map<String, String> map = new HashMap<>();
            map.put(COLUMNNAME, cond_en_column);
            map.put(ZUIDAXIAOKEY, ZUIDANGE);
            map.put(LIMITVALUE, cond_value);
            upAndLowArray.add(map);
        } else if (AutoDataOperator.ZuiXiaoDeNGe == AutoDataOperator.ofEnumByCode(operator)) {
            Map<String, String> map = new HashMap<>();
            map.put(COLUMNNAME, cond_en_column);
            map.put(ZUIDAXIAOKEY, ZUIXIAONGE);
            map.put(LIMITVALUE, cond_value);
            upAndLowArray.add(map);
        } else if (AutoDataOperator.WeiKong == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + "IS NULL" + Constant.SPACE;
        } else if (AutoDataOperator.FeiKong == AutoDataOperator.ofEnumByCode(operator)) {
            return cond_en_column + Constant.SPACE + "IS NOT NULL" + Constant.SPACE;
        } else {
            throw new BusinessException("当前操作属性的代码项:" + operator + ",不存在于过滤关系中");
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getAccessTemplateInfo(Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> accessTepList = Dbo.queryPagedList(page, "select t1.template_id,t1.template_name,t1.template_desc,t1.create_date,t1.create_time," + "t1.create_user,count(t2.fetch_sum_id) as count_number " + " from " + AutoTpInfo.TableName + " t1 left join " + AutoFetchSum.TableName + " t2" + " on t1.template_id = t2.template_id " + " where template_status = ? group by t1.template_id " + " order by t1.create_date desc,t1.create_time desc", AutoTemplateStatus.FaBu.getCode());
        Map<String, Object> accessTepMap = new HashMap<>();
        accessTepMap.put("accessTepList", accessTepList);
        accessTepMap.put("totalSize", page.getTotalSize());
        return accessTepMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_name", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getAccessTemplateInfoByName(String template_name, Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> accessTepList = Dbo.queryPagedList(page, "select t1.template_id,t1.template_name,t1.template_desc,t1.create_date,t1.create_time," + "t1.create_user,count(t2.fetch_sum_id) as count_number " + " from " + AutoTpInfo.TableName + " t1 left join " + AutoFetchSum.TableName + " t2" + " on t1.template_id = t2.template_id " + " where template_status = ? and template_name like ?" + " group by t1.template_id order by t1.create_date desc,t1.create_time desc", AutoTemplateStatus.FaBu.getCode(), "%" + template_name + "%");
        Map<String, Object> accessTepMap = new HashMap<>();
        accessTepMap.put("accessTepList", accessTepList);
        accessTepMap.put("totalSize", page.getTotalSize());
        return accessTepMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_name", desc = "", range = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getMyAccessInfoByName(String fetch_name, Integer currPage, Integer pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> myAccessList = Dbo.queryPagedList(page, "select * from " + AutoFetchSum.TableName + " where create_user = ? and fetch_name like ?" + " order by create_date desc,create_time desc", UserUtil.getUserId(), "%" + fetch_name + "%");
        Map<String, Object> myAccessMap = new HashMap<>();
        myAccessMap.put("myAccessList", myAccessList);
        myAccessMap.put("totalSize", page.getTotalSize());
        return myAccessMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAccessTemplateInfoById(long template_id) {
        return Dbo.queryOneObject("select template_name,template_desc from " + AutoTpInfo.TableName + " where template_id = ?", template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAccessResultFields(long template_id) {
        return Dbo.queryList("select res_show_column,template_res_id from " + AutoTpResSet.TableName + " where template_id = ?", template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAccessSelectHistory(long template_id) {
        Page page = new DefaultPageImpl(1, 10);
        return Dbo.queryPagedList(page, "select * from " + AutoFetchSum.TableName + " where template_id = ? " + " order by create_date desc ,create_time desc", template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAutoAccessFilterCond(long template_id) {
        return Dbo.queryList("select * from " + AutoTpCondInfo.TableName + " where template_id = ?", template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAccessResultFromHistory(long fetch_sum_id) {
        return Dbo.queryList("select distinct t1.fetch_res_name as res_show_column,t1.fetch_res_id,t1.template_res_id" + " from " + AutoFetchRes.TableName + " t1 left join " + AutoFetchSum.TableName + " t2 " + " on t1.fetch_sum_id=t2.fetch_sum_id left join " + AutoTpResSet.TableName + " t3" + " on t2.template_id = t3.template_id left join " + AutoTpInfo.TableName + " t4" + " on t3.template_id = t4.template_id where t2.fetch_sum_id = ?", fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_fetch_sum", desc = "", range = "", isBean = true)
    @Param(name = "autoTpCondInfos", desc = "", range = "", isBean = true)
    @Param(name = "autoFetchRes", desc = "", range = "", isBean = true)
    public Long saveAutoAccessInfoToQuery(AutoFetchSum auto_fetch_sum, AutoTpCondInfo[] autoTpCondInfos, AutoFetchRes[] autoFetchRes) {
        Validator.notNull(auto_fetch_sum.getTemplate_id(), "模板ID不能为空");
        isAutoTpInfoExist(auto_fetch_sum.getTemplate_id());
        auto_fetch_sum.setFetch_sum_id(PrimayKeyGener.getNextId());
        auto_fetch_sum.setCreate_date(DateUtil.getSysDate());
        auto_fetch_sum.setCreate_time(DateUtil.getSysTime());
        auto_fetch_sum.setCreate_user(UserUtil.getUserId());
        auto_fetch_sum.setFetch_sql(getWhereSql(auto_fetch_sum.getTemplate_id(), autoTpCondInfos, autoFetchRes));
        auto_fetch_sum.setFetch_status(AutoFetchStatus.WanCheng.getCode());
        auto_fetch_sum.add(Dbo.db());
        for (AutoTpCondInfo auto_tp_cond_info : autoTpCondInfos) {
            Validator.notBlank(auto_tp_cond_info.getPre_value(), "条件值不能为空");
            Validator.notNull(auto_tp_cond_info.getTemplate_cond_id(), "模板条件ID不能为空");
            AutoFetchCond auto_fetch_cond = new AutoFetchCond();
            auto_fetch_cond.setFetch_cond_id(PrimayKeyGener.getNextId());
            auto_fetch_cond.setFetch_sum_id(auto_fetch_sum.getFetch_sum_id());
            auto_fetch_cond.setTemplate_cond_id(auto_tp_cond_info.getTemplate_cond_id());
            auto_fetch_cond.setCond_value(auto_tp_cond_info.getPre_value());
            auto_fetch_cond.add(Dbo.db());
        }
        for (AutoFetchRes auto_fetch_res : autoFetchRes) {
            Validator.notNull(auto_fetch_res.getTemplate_res_id(), "模板结果ID不能为空");
            List<String> res_show_column = Dbo.queryOneColumnList("select res_show_column from " + AutoTpResSet.TableName + " where template_res_id = ?", auto_fetch_res.getTemplate_res_id());
            auto_fetch_res.setFetch_res_name(res_show_column.get(0));
            auto_fetch_res.setShow_num(auto_fetch_res.getShow_num() == null ? 0 : auto_fetch_res.getShow_num());
            auto_fetch_res.setFetch_res_id(PrimayKeyGener.getNextId());
            auto_fetch_res.setFetch_sum_id(auto_fetch_sum.getFetch_sum_id());
            auto_fetch_res.add(Dbo.db());
        }
        return auto_fetch_sum.getFetch_sum_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    private void isAutoTpInfoExist(long template_id) {
        if (Dbo.queryNumber("select count(1) from " + AutoTpInfo.TableName + " where template_id=?", template_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException("当前模板ID:" + template_id + "对应模板信息已不存在");
        }
    }

    private String getWhereSql(long template_id, AutoTpCondInfo[] autoTpCondInfos, AutoFetchRes[] autoFetchRes) {
        SqlOperator.Assembler assembler = SqlOperator.Assembler.newInstance();
        StringBuilder resultSql = new StringBuilder("select ");
        List<String> template_sql = Dbo.queryOneColumnList("select template_sql from auto_tp_info where template_id = ?", template_id);
        DbType postgresql = JdbcConstants.POSTGRESQL;
        String format_sql = SQLUtils.format(template_sql.get(0), postgresql);
        List<AutoTpCondInfo> autoTpCondInfoList = Dbo.queryList(AutoTpCondInfo.class, "select * from " + AutoTpCondInfo.TableName + " where template_id = ?", template_id);
        for (int i = 0; i < autoTpCondInfoList.size(); i++) {
            AutoTpCondInfo auto_tp_cond_info = autoTpCondInfoList.get(i);
            AutoTpCondInfo autoTpCondInfo = autoTpCondInfos[i];
            String condParam;
            String newParam;
            if (auto_tp_cond_info.getTemplate_cond_id().equals(autoTpCondInfo.getTemplate_cond_id())) {
                if (auto_tp_cond_info.getCon_relation().equals("IN")) {
                    condParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + auto_tp_cond_info.getCon_relation() + Constant.SPACE + Constant.LXKH + auto_tp_cond_info.getPre_value().replace(",", ", ") + Constant.RXKH;
                    newParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + autoTpCondInfo.getCon_relation() + Constant.SPACE + Constant.LXKH + autoTpCondInfo.getPre_value().replace(",", ", ") + Constant.RXKH;
                } else if (auto_tp_cond_info.getCon_relation().equals("BETWEEN")) {
                    condParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + auto_tp_cond_info.getCon_relation() + Constant.SPACE + auto_tp_cond_info.getPre_value().replace(",", " AND ");
                    newParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + autoTpCondInfo.getCon_relation() + Constant.SPACE + autoTpCondInfo.getPre_value().replace(",", " AND ");
                } else {
                    condParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + auto_tp_cond_info.getCon_relation() + Constant.SPACE + auto_tp_cond_info.getPre_value();
                    newParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + autoTpCondInfo.getCon_relation() + Constant.SPACE + autoTpCondInfo.getPre_value();
                }
                if (!format_sql.contains(condParam)) {
                    format_sql = trim(format_sql);
                }
                format_sql = StringUtil.replace(format_sql, condParam, newParam);
            } else {
                condParam = auto_tp_cond_info.getCond_para_name() + Constant.SPACE + auto_tp_cond_info.getCon_relation() + Constant.SPACE + auto_tp_cond_info.getPre_value();
                format_sql = StringUtil.replace(format_sql, condParam, "");
            }
        }
        for (AutoFetchRes auto_fetch_res : autoFetchRes) {
            AutoTpResSet auto_tp_res_set = Dbo.queryOneObject(AutoTpResSet.class, "select * from " + AutoTpResSet.TableName + " where template_res_id = ?", auto_fetch_res.getTemplate_res_id()).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
            String column_en_name = auto_tp_res_set.getColumn_en_name();
            String res_show_column = auto_tp_res_set.getRes_show_column();
            if (StringUtil.isNotBlank(res_show_column) || !column_en_name.equals(res_show_column)) {
                resultSql.append(res_show_column).append(",");
            } else {
                resultSql.append(column_en_name).append(",");
            }
        }
        resultSql = new StringBuilder(resultSql.substring(0, resultSql.length() - 1));
        resultSql.append(" from (").append(format_sql).append(") ").append(TempTableName);
        assembler.addSql(resultSql.toString());
        return assembler.sql();
    }

    private String trim(String string) {
        while (string.contains("\n") || string.contains("\r") || string.contains("\n\r") || string.contains("\t") || string.contains(Constant.SPACE + Constant.SPACE) || string.contains(Constant.SPACE + Constant.RXKH) || string.contains(Constant.LXKH + Constant.SPACE)) {
            string = string.replace("\n", Constant.SPACE);
            string = string.replace("\r", Constant.SPACE);
            string = string.replace("\n\r", Constant.SPACE);
            string = string.replace("\t", Constant.SPACE);
            string = string.replace(Constant.SPACE + Constant.SPACE, Constant.SPACE);
            string = string.replace(Constant.SPACE + Constant.RXKH, Constant.RXKH);
            string = string.replace(Constant.LXKH + Constant.SPACE, Constant.LXKH);
        }
        return string;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "category", desc = "", range = "")
    @Return(desc = "", range = "")
    public Result getCategoryItems(String category) {
        return WebCodesItem.getCategoryItems(category);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAccessCondFromHistory(long fetch_sum_id) {
        return Dbo.queryList("select t1.*,t2.* from " + AutoFetchCond.TableName + " t1 left join " + AutoTpCondInfo.TableName + " t2 on t1.template_cond_id = t2.template_cond_id" + " left join " + AutoFetchSum.TableName + " t3 on t1.fetch_sum_id = t3.fetch_sum_id" + " left join " + AutoTpInfo.TableName + " t4 on t2.template_id = t4.template_id" + " where t3.fetch_sum_id = ?", fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAutoAccessQueryResult(long fetch_sum_id) {
        String fetch_sql = getAccessSql(fetch_sum_id);
        List<Map<String, Object>> accessResult = new ArrayList<>();
        new ProcessingData() {

            @Override
            public void dealLine(Map<String, Object> map) {
                accessResult.add(map);
            }
        }.getPageDataLayer(fetch_sql, Dbo.db(), 1, 50);
        return accessResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAccessResultByNumber(Long fetch_sum_id, Integer showNum) {
        String accessSql = getAccessSql(fetch_sum_id);
        List<Map<String, Object>> resultData = new ArrayList<>();
        if (showNum > 1000) {
            showNum = 1000;
        }
        new ProcessingData() {

            @Override
            public void dealLine(Map<String, Object> map) {
                resultData.add(map);
            }
        }.getPageDataLayer(accessSql, Dbo.db(), 1, showNum <= 0 ? 50 : showNum);
        return resultData;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "auto_fetch_sum", desc = "", range = "", isBean = true)
    public void saveAutoAccessInfo(AutoFetchSum auto_fetch_sum) {
        Validator.notNull(auto_fetch_sum.getTemplate_id(), "模板ID不能为空");
        Validator.notNull(auto_fetch_sum.getFetch_sum_id(), "取数汇总ID不能为空");
        Validator.notNull(auto_fetch_sum.getFetch_name(), "取数名称不能为空");
        if (Dbo.queryNumber(Dbo.db(), "select count(*) from " + AutoFetchSum.TableName + " where fetch_name=?", auto_fetch_sum.getFetch_name()).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("取数主题名称已存在");
        }
        auto_fetch_sum.setUpdate_user(UserUtil.getUserId());
        auto_fetch_sum.setFetch_status(AutoFetchStatus.WanCheng.getCode());
        auto_fetch_sum.setLast_update_date(DateUtil.getSysDate());
        auto_fetch_sum.setLast_update_time(DateUtil.getSysTime());
        try {
            auto_fetch_sum.update(Dbo.db());
        } catch (Exception e) {
            if (!(e instanceof ProEntity.EntityDealZeroException)) {
                throw new BusinessException("更新自主取数汇总数据失败");
            }
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getMyAccessInfoById(long fetch_sum_id) {
        return Dbo.queryOneObject("select * from " + AutoFetchSum.TableName + " where fetch_sum_id = ?", fetch_sum_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "fetch_sum_id", desc = "", range = "")
    public void downloadMyAccessTemplate(long fetch_sum_id) {
        AutoFetchSum auto_fetch_sum = Dbo.queryOneObject(AutoFetchSum.class, "select fetch_sql,fetch_name from " + AutoFetchSum.TableName + " where fetch_sum_id = ?", fetch_sum_id).orElseThrow(() -> new BusinessException("sql查询错误或者映射实体失败"));
        String fileName = WebinfoProperties.FileUpload_SavedDirName + File.separator + auto_fetch_sum.getFetch_name() + Constant.SPLITTER + DateUtil.getDateTime().replace(Constant.SPACE, Constant.SPLITTER) + ".csv";
        new ProcessingData() {

            @Override
            public void dealLine(Map<String, Object> map) {
                AutoOperateCommon.writeFile(map, fileName);
            }
        }.getDataLayer(auto_fetch_sum.getFetch_sql(), Dbo.db());
        AutoOperateCommon.lineCounter = 0;
    }
}
