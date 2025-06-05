package hyren.serv6.v.manage;

import com.alibaba.druid.DbType;
import com.alibaba.druid.sql.SQLUtils;
import com.alibaba.druid.sql.ast.SQLExpr;
import com.alibaba.druid.sql.ast.expr.*;
import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import com.alibaba.druid.util.JdbcConstants;
import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.*;
import fd.ng.db.jdbc.DefaultPageImpl;
import fd.ng.db.jdbc.Page;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.AutoTemplateStatus;
import hyren.serv6.base.codes.AutoValueType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.entity.AutoTpCondInfo;
import hyren.serv6.base.entity.AutoTpInfo;
import hyren.serv6.base.entity.AutoTpResSet;
import hyren.serv6.base.entity.fdentity.ProEntity;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.utils.DboExecute;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.v.common.VisualizationParam;
import io.swagger.annotations.Api;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.util.*;

@RestController
@RequestMapping("/dataVisualization/manage/")
@Slf4j
@Api(tags = "")
@Validated
public class ManageController {

    @Autowired
    private ManageService manageService;

    private static final Logger logger = LogManager.getLogger();

    private static final ArrayList<String> numbersArray = new ArrayList<>();

    private static final int maxvaluesize = 64;

    static {
        numbersArray.add("int");
        numbersArray.add("int8");
        numbersArray.add("int16");
        numbersArray.add("integer");
        numbersArray.add("tinyint");
        numbersArray.add("smallint");
        numbersArray.add("mediumint");
        numbersArray.add("bigint");
        numbersArray.add("float");
        numbersArray.add("double");
        numbersArray.add("decimal");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public Map<String, Object> getTemplateConfInfo(int currPage, int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> tpInfoList = Dbo.queryPagedList(page, "select * from " + AutoTpInfo.TableName + " where template_status != ? " + " order by create_date desc,create_time desc", AutoTemplateStatus.ZhuXiao.getCode());
        Map<String, Object> tpInfoMap = new HashMap<>();
        tpInfoMap.put("tpInfoList", tpInfoList);
        tpInfoMap.put("totalSize", page.getTotalSize());
        return tpInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "currPage", desc = "", range = "", valueIfNull = "1")
    @Param(name = "pageSize", desc = "", range = "", valueIfNull = "10")
    @Param(name = "template_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getTemplateConfInfoByName(String template_name, int currPage, int pageSize) {
        Page page = new DefaultPageImpl(currPage, pageSize);
        List<Map<String, Object>> tpInfoList = Dbo.queryPagedList(page, "select * from " + AutoTpInfo.TableName + " where template_name like ? " + " order by create_date desc,create_time desc", "%" + template_name + "%");
        Map<String, Object> tpInfoMap = new HashMap<>();
        tpInfoMap.put("tpInfoList", tpInfoList);
        tpInfoMap.put("totalSize", page.getTotalSize());
        return tpInfoMap;
    }

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    @PostMapping("/getAutoAnalysisTreeData")
    public List<Node> getAutoAnalysisTreeData() {
        return manageService.getAutoAnalysisTreeData();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_sql", desc = "", range = "")
    public void verifySqlIsLegal(String template_sql) {
        try {
            if (template_sql.endsWith(";")) {
                throw new BusinessException("sql不要使用;结尾");
            }
            new ProcessingData() {

                @Override
                public void dealLine(Map<String, Object> map) {
                }
            }.getPageDataLayer(template_sql, Dbo.db(), 1, 10);
        } catch (Exception e) {
            logger.error(e);
            throw new BusinessException("sql错误：" + e.getMessage());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_sql", desc = "", range = "")
    public Map<String, Object> generateTemplateParam(String template_sql) {
        verifySqlIsLegal(template_sql);
        DbType oracle = JdbcConstants.ORACLE;
        String format_sql = SQLUtils.format(template_sql, oracle).trim();
        if (format_sql.endsWith(";")) {
            format_sql = format_sql.substring(0, format_sql.length() - 1);
        }
        Set<Map<String, Object>> autoTpCondInfoList = new HashSet<>();
        Set<AutoTpResSet> autoTpResSets = new HashSet<>();
        DruidParseQuerySql druidParseQuerySql = new DruidParseQuerySql(format_sql);
        List<SQLExpr> allWhereList = druidParseQuerySql.getAllWherelist();
        for (SQLExpr sqlExpr : allWhereList) {
            setAutoTpCond(autoTpCondInfoList, sqlExpr);
        }
        getAutoTpResSet(autoTpResSets, format_sql, druidParseQuerySql.selectList);
        Map<String, Object> templateMap = new HashMap<>();
        templateMap.put("autoTpCondInfo", autoTpCondInfoList);
        templateMap.put("autoTpResSetInfo", autoTpResSets);
        return templateMap;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoTpCondInfos", desc = "", range = "", isBean = true)
    @Param(name = "autoTpResSets", desc = "", range = "", isBean = true)
    @Param(name = "auto_tp_info", desc = "", range = "", isBean = true)
    @PostMapping("/saveTemplateConfInfo")
    public Long saveTemplateConfInfo(@RequestBody VisualizationParam visualizationParam) {
        AutoTpInfo auto_tp_info = new AutoTpInfo();
        BeanUtil.copyProperties(visualizationParam, auto_tp_info);
        checkAutoTpInfoFields(auto_tp_info);
        isTemplateNameExist(visualizationParam.getTemplate_name());
        auto_tp_info.setTemplate_status(AutoTemplateStatus.BianJi.getCode());
        auto_tp_info.setTemplate_id(PrimayKeyGener.getNextId());
        auto_tp_info.setCreate_date(DateUtil.getSysDate());
        auto_tp_info.setCreate_time(DateUtil.getSysTime());
        auto_tp_info.setCreate_user(UserUtil.getUserId());
        auto_tp_info.add(Dbo.db());
        addAutoTpCondInfo(visualizationParam.getAutoTpCondInfos(), auto_tp_info.getTemplate_id());
        addAutoTpResSet(visualizationParam.getAutoTpResSets(), auto_tp_info.getTemplate_id());
        return auto_tp_info.getTemplate_id();
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoTpResSets", desc = "", range = "", isBean = true)
    @Param(name = "template_id", desc = "", range = "")
    private void addAutoTpResSet(List<AutoTpResSet> autoTpResSets, long template_id) {
        for (AutoTpResSet auto_tp_res_set : autoTpResSets) {
            checkAutoTpResSetFields(auto_tp_res_set);
            auto_tp_res_set.setTemplate_res_id(PrimayKeyGener.getNextId());
            auto_tp_res_set.setTemplate_id(template_id);
            auto_tp_res_set.setIs_dese(IsFlag.Fou.getCode());
            auto_tp_res_set.setCreate_date(DateUtil.getSysDate());
            auto_tp_res_set.setCreate_time(DateUtil.getSysTime());
            auto_tp_res_set.setCreate_user(UserUtil.getUserId());
            auto_tp_res_set.add(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoTpCondInfos", desc = "", range = "", isBean = true)
    @Param(name = "template_id", desc = "", range = "")
    private void addAutoTpCondInfo(List<AutoTpCondInfo> autoTpCondInfos, long template_id) {
        for (int i = 0; i < autoTpCondInfos.size(); i++) {
            AutoTpCondInfo auto_tp_cond_info = autoTpCondInfos.get(i);
            checkAutoTpCondInfoFields(auto_tp_cond_info);
            auto_tp_cond_info.setTemplate_cond_id(PrimayKeyGener.getNextId());
            auto_tp_cond_info.setTemplate_id(template_id);
            auto_tp_cond_info.setIs_dept_id(IsFlag.Fou.getCode());
            auto_tp_cond_info.setValue_size(auto_tp_cond_info.getValue_size() == null ? String.valueOf(64) : auto_tp_cond_info.getValue_size());
            auto_tp_cond_info.setCon_row(String.valueOf(i));
            auto_tp_cond_info.add(Dbo.db());
        }
    }

    private void checkAutoTpResSetFields(AutoTpResSet auto_tp_res_set) {
        Validator.notBlank(auto_tp_res_set.getColumn_cn_name(), "字段中文名不能为空");
        Validator.notBlank(auto_tp_res_set.getColumn_en_name(), "字段英文名不能为空");
        Validator.notBlank(auto_tp_res_set.getColumn_type(), "字段类型不能为空");
        Validator.notBlank(auto_tp_res_set.getRes_show_column(), "结果显示字段不能为空");
        Validator.notBlank(auto_tp_res_set.getSource_table_name(), "字段来源表名不能为空");
    }

    private void checkAutoTpCondInfoFields(AutoTpCondInfo auto_tp_cond_info) {
        Validator.notBlank(auto_tp_cond_info.getCond_en_column(), "条件对应的英文字段不能为空");
        Validator.notBlank(auto_tp_cond_info.getCond_cn_column(), "条件对应的中文字段不能为空");
        Validator.notBlank(auto_tp_cond_info.getCond_para_name(), "条件参数名称不能为空");
        Validator.notBlank(auto_tp_cond_info.getCon_relation(), "条件参数名称不能为空");
        Validator.notBlank(auto_tp_cond_info.getIs_required(), "是否必填不能为空");
    }

    private void checkAutoTpInfoFields(AutoTpInfo auto_tp_info) {
        Validator.notBlank(auto_tp_info.getTemplate_name(), "模板名称不能为空");
        IsFlag.ofEnumByCode(auto_tp_info.getData_source());
        Validator.notBlank(auto_tp_info.getTemplate_sql(), "模板sql不能为空");
        Validator.notBlank(auto_tp_info.getTemplate_desc(), "模板描述不能为空");
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> getAutoTpInfoById(long template_id) {
        return Dbo.queryOneObject("select * from " + AutoTpInfo.TableName + " where template_id=?", template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAutoTpCondInfoById(long template_id) {
        isExistAutoTpInfo(template_id);
        Map<String, Object> autoTpInfo = getAutoTpInfoById(template_id);
        Map<String, Object> paramMap = generateTemplateParam(autoTpInfo.get("template_sql").toString());
        List<Map<String, Object>> autoTpCondInfoList = JsonUtil.toObject(paramMap.get("autoTpCondInfo").toString(), new TypeReference<List<Map<String, Object>>>() {
        });
        List<Map<String, Object>> auto_tp_cond_infoList = Dbo.queryList("select * from " + AutoTpCondInfo.TableName + " where template_id=?", template_id);
        for (Map<String, Object> objectMap : autoTpCondInfoList) {
            objectMap.put("checked", false);
            for (Map<String, Object> map : auto_tp_cond_infoList) {
                if (map.containsValue(objectMap.get("cond_para_name"))) {
                    objectMap.put("value_type", map.get("value_type"));
                    objectMap.put("value_size", map.get("value_size"));
                    objectMap.put("pre_value", map.get("pre_value"));
                    objectMap.put("is_required", map.get("is_required"));
                    objectMap.put("checked", true);
                }
            }
        }
        return autoTpCondInfoList;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getAutoTpResSetById(long template_id) {
        isExistAutoTpInfo(template_id);
        return Dbo.queryList("select * from " + AutoTpResSet.TableName + " where template_id=?", template_id);
    }

    private void isExistAutoTpInfo(long template_id) {
        if (Dbo.queryNumber("select count(*) from " + AutoTpInfo.TableName + " where template_id=?", template_id).orElseThrow(() -> new BusinessException("sql查询错误")) == 0) {
            throw new BusinessException(template_id + "对应模板不存在，请检查");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "autoTpCondInfos", desc = "", range = "", isBean = true)
    @Param(name = "autoTpResSets", desc = "", range = "", isBean = true)
    @Param(name = "auto_tp_info", desc = "", range = "", isBean = true)
    @PostMapping("/updateTemplateConfInfo")
    public void updateTemplateConfInfo(@RequestBody VisualizationParam visualizationParam) {
        AutoTpInfo auto_tp_info = new AutoTpInfo();
        BeanUtil.copyProperties(visualizationParam, auto_tp_info);
        Validator.notNull(auto_tp_info.getTemplate_id(), "编辑时模板ID不能为空");
        Validator.notNull(auto_tp_info.getTemplate_status(), "编辑时模板状态不能为空");
        checkAutoTpInfoFields(auto_tp_info);
        checkAutoTemplateStatus(auto_tp_info.getTemplate_status());
        auto_tp_info.setUpdate_user(UserUtil.getUserId());
        auto_tp_info.setLast_update_date(DateUtil.getSysDate());
        auto_tp_info.setLast_update_time(DateUtil.getSysTime());
        auto_tp_info.setTemplate_sql(auto_tp_info.getTemplate_sql().replace(";", ""));
        try {
            auto_tp_info.update(Dbo.db());
        } catch (Exception e) {
            if (!(e instanceof ProEntity.EntityDealZeroException)) {
                logger.error(e);
                throw new BusinessException(e.getMessage());
            }
        }
        Dbo.execute("delete from " + AutoTpCondInfo.TableName + " where template_id=?", auto_tp_info.getTemplate_id());
        Dbo.execute("delete from " + AutoTpResSet.TableName + " where template_id=?", auto_tp_info.getTemplate_id());
        addAutoTpCondInfo(visualizationParam.getAutoTpCondInfos(), auto_tp_info.getTemplate_id());
        addAutoTpResSet(visualizationParam.getAutoTpResSets(), auto_tp_info.getTemplate_id());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_status", desc = "", range = "")
    private void checkAutoTemplateStatus(String template_status) {
        if (AutoTemplateStatus.BianJi != AutoTemplateStatus.ofEnumByCode(template_status)) {
            throw new BusinessException("自主取数模板状态不为编辑：" + template_status);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_sql", desc = "", range = "")
    @Param(name = "showNum", desc = "", range = "", valueIfNull = "10")
    @Return(desc = "", range = "")
    public List<Map<String, Object>> getPreviewData(String template_sql, String showNum) {
        List<Map<String, Object>> resultData = new ArrayList<>();
        int i;
        try {
            i = Integer.parseInt(showNum);
        } catch (Exception e) {
            throw new BusinessException("请填入整数");
        }
        if (i > 1000) {
            i = 1000;
        }
        new ProcessingData() {

            @Override
            public void dealLine(Map<String, Object> map) {
                resultData.add(map);
            }
        }.getPageDataLayer(template_sql, Dbo.db(), 1, i <= 0 ? 100 : i);
        return resultData;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    @PostMapping("/releaseAutoAnalysisTemplate")
    public void releaseAutoAnalysisTemplate(long template_id) {
        DboExecute.updatesOrThrow("更新自主取数模板状态为发布失败", "update " + AutoTpInfo.TableName + " set template_status=? where template_id=?", AutoTemplateStatus.FaBu.getCode(), template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_id", desc = "", range = "")
    public void deleteAutoAnalysisTemplate(long template_id) {
        DboExecute.updatesOrThrow("更新自主取数模板状态为注销失败", "update " + AutoTpInfo.TableName + " set template_status=? where template_id=?", AutoTemplateStatus.ZhuXiao.getCode(), template_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "template_name", desc = "", range = "")
    private void isTemplateNameExist(String template_name) {
        if (Dbo.queryNumber("select count(1) from " + AutoTpInfo.TableName + " where template_name = ? and template_status != ?", template_name, AutoTemplateStatus.ZhuXiao.getCode()).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            throw new BusinessException("模板名称已存在");
        }
    }

    private void setAutoTpCond(Set<Map<String, Object>> autoTpCondInfoList, SQLExpr sqlExpr) {
        if (sqlExpr instanceof SQLBetweenExpr) {
            SQLBetweenExpr sqlBetweenExpr = (SQLBetweenExpr) sqlExpr;
            SQLExpr testExpr = sqlBetweenExpr.getTestExpr();
            SQLExpr beginExpr = sqlBetweenExpr.getBeginExpr();
            SQLExpr endExpr = sqlBetweenExpr.getEndExpr();
            String pre_value = beginExpr.toString() + "," + endExpr.toString();
            setAutoTpCondBySqlExpr(autoTpCondInfoList, testExpr, "BETWEEN", pre_value, IsFlag.Shi.getCode());
        } else if (sqlExpr instanceof SQLBinaryOpExpr) {
            SQLBinaryOpExpr sqlBinaryOpExpr = (SQLBinaryOpExpr) sqlExpr;
            SQLExpr exprLeft = sqlBinaryOpExpr.getLeft();
            SQLExpr exprRight = sqlBinaryOpExpr.getRight();
            if (checkExprIfColumn(exprLeft) || checkExprIfColumn(exprRight)) {
                if (checkExprIfColumn(exprLeft) && checkExprIfColumn(exprRight)) {
                    setAutoTpCondBySqlExpr(autoTpCondInfoList, exprLeft, sqlBinaryOpExpr.getOperator().getName(), exprRight.toString(), IsFlag.Fou.getCode());
                } else if (checkExprIfColumn(exprLeft)) {
                    setAutoTpCondBySqlExpr(autoTpCondInfoList, exprLeft, sqlBinaryOpExpr.getOperator().getName(), exprRight.toString(), IsFlag.Shi.getCode());
                } else {
                    setAutoTpCondBySqlExpr(autoTpCondInfoList, exprRight, sqlBinaryOpExpr.getOperator().getName(), exprLeft.toString(), IsFlag.Shi.getCode());
                }
            } else {
                logger.info("sqlExpr:" + sqlExpr.toString() + " 的左右两侧都不是字段，跳过");
            }
        } else if (sqlExpr instanceof SQLInListExpr) {
            SQLInListExpr sqlInListExpr = (SQLInListExpr) sqlExpr;
            SQLExpr leftExpr = sqlInListExpr.getExpr();
            List<SQLExpr> targetList = sqlInListExpr.getTargetList();
            StringBuilder sb = new StringBuilder();
            for (SQLExpr expr : targetList) {
                sb.append(expr.toString()).append(",");
            }
            String pre_value = sb.deleteCharAt(sb.length() - 1).toString();
            setAutoTpCondBySqlExpr(autoTpCondInfoList, leftExpr, "IN", pre_value, IsFlag.Shi.getCode());
        } else {
            if (sqlExpr != null) {
                Class<? extends SQLExpr> aClass = sqlExpr.getClass();
                throw new BusinessException("sqlexpr：" + sqlExpr.toString() + "sqlexpr.class:" + aClass + " 请联系管理员");
            }
        }
    }

    private boolean checkExprIfColumn(SQLExpr expr) {
        return expr instanceof SQLIdentifierExpr || expr instanceof SQLPropertyExpr;
    }

    private void setAutoTpCondBySqlExpr(Set<Map<String, Object>> autoTpCondInfoList, SQLExpr sqlExpr, String con_relation, String pre_value, String is_required) {
        Map<String, Object> condInfoMap = new HashMap<>();
        condInfoMap.put("value_type", AutoValueType.ZiFuChuan.getCode());
        if (sqlExpr instanceof SQLPropertyExpr) {
            SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlExpr;
            condInfoMap.put("cond_para_name", sqlPropertyExpr.toString());
            condInfoMap.put("cond_en_column", sqlPropertyExpr.getName());
            condInfoMap.put("cond_cn_column", sqlPropertyExpr.getName());
        } else {
            condInfoMap.put("cond_para_name", sqlExpr.toString());
            condInfoMap.put("cond_en_column", sqlExpr.toString());
            condInfoMap.put("cond_cn_column", sqlExpr.toString());
        }
        condInfoMap.put("con_relation", con_relation);
        condInfoMap.put("is_required", is_required);
        condInfoMap.put("value_size", maxvaluesize);
        condInfoMap.put("pre_value", pre_value);
        condInfoMap.put("checked", true);
        if (autoTpCondInfoList.contains(condInfoMap)) {
            throw new BusinessException("存在相同的条件：" + condInfoMap.get("cond_en_column") + " " + condInfoMap.get("con_relation") + " " + condInfoMap.get("pre_value") + ",无法解析，建议先通过加工生成目标表后再创建模板表");
        }
        autoTpCondInfoList.add(condInfoMap);
    }

    private void getAutoTpResSet(Set<AutoTpResSet> autoTpResSets, String sql, List<SQLSelectItem> selectList) {
        Map<String, String> sourceAndAliasName = getTableAndAliasName(sql);
        for (SQLSelectItem sqlSelectItem : selectList) {
            String expr = sqlSelectItem.getExpr().toString();
            String alias = sqlSelectItem.getAlias();
            if (sqlSelectItem.getExpr() instanceof SQLAllColumnExpr) {
                getAllColumnsBySql(autoTpResSets, sql);
            } else if (sqlSelectItem.getExpr() instanceof SQLPropertyExpr) {
                SQLPropertyExpr sqlPropertyExpr = (SQLPropertyExpr) sqlSelectItem.getExpr();
                String column_en_name = sqlPropertyExpr.getName();
                String table_alias = sqlPropertyExpr.getOwner().toString().toUpperCase();
                String table_name = sourceAndAliasName.get(table_alias);
                List<Map<String, Object>> columns = DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
                if (column_en_name.equals("*")) {
                    setAllColumns(autoTpResSets, table_name, columns);
                } else {
                    AutoTpResSet auto_tp_res_set = new AutoTpResSet();
                    auto_tp_res_set.setSource_table_name(table_name);
                    auto_tp_res_set.setColumn_en_name(column_en_name);
                    auto_tp_res_set.setRes_show_column(StringUtil.isBlank(alias) ? column_en_name : alias);
                    setAutoTpResSet(autoTpResSets, auto_tp_res_set, columns);
                }
            } else if (sqlSelectItem.getExpr() instanceof SQLAggregateExpr) {
                AutoTpResSet auto_tp_res_set = new AutoTpResSet();
                auto_tp_res_set.setSource_table_name("UNKNOW");
                auto_tp_res_set.setColumn_en_name(expr);
                auto_tp_res_set.setColumn_cn_name(expr);
                auto_tp_res_set.setRes_show_column(StringUtil.isBlank(alias) ? expr : alias);
                auto_tp_res_set.setColumn_type(AutoValueType.ShuZhi.getCode());
                autoTpResSets.add(auto_tp_res_set);
            } else if (sqlSelectItem.getExpr() instanceof SQLIdentifierExpr) {
                SQLIdentifierExpr sqlIdentifierExpr = (SQLIdentifierExpr) sqlSelectItem.getExpr();
                AutoTpResSet auto_tp_res_set = new AutoTpResSet();
                String exprName = sqlIdentifierExpr.getName();
                auto_tp_res_set.setColumn_en_name(exprName);
                auto_tp_res_set.setRes_show_column(StringUtil.isBlank(alias) ? exprName : alias);
                List<String> tableList = DruidParseQuerySql.parseSqlTableToList(sql);
                Map<String, String> columnByTable = getColumnByTable(tableList);
                for (String key : columnByTable.keySet()) {
                    if (key.equalsIgnoreCase(exprName)) {
                        String tableAndChColumnName = columnByTable.get(key);
                        List<String> tableAndColumn = StringUtil.split(tableAndChColumnName, Constant.METAINFOSPLIT);
                        auto_tp_res_set.setSource_table_name(tableAndColumn.get(0));
                        auto_tp_res_set.setColumn_cn_name(tableAndColumn.get(1));
                        if (numbersArray.contains(tableAndColumn.get(2))) {
                            auto_tp_res_set.setColumn_type(AutoValueType.ShuZhi.getCode());
                        } else {
                            auto_tp_res_set.setColumn_type(AutoValueType.ZiFuChuan.getCode());
                        }
                        autoTpResSets.add(auto_tp_res_set);
                    }
                }
            } else {
                throw new BusinessException(sqlSelectItem.getExpr() + "未开发 有待开发");
            }
        }
    }

    private Map<String, String> getColumnByTable(List<String> tableList) {
        Map<String, String> columnByTable = new HashMap<>();
        for (String table_name : tableList) {
            List<Map<String, Object>> columns = DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
            for (Map<String, Object> column : columns) {
                columnByTable.put(column.get("column_name").toString(), table_name + Constant.METAINFOSPLIT + column.get("column_ch_name").toString() + Constant.METAINFOSPLIT + column.get("data_type").toString());
            }
        }
        return columnByTable;
    }

    private void getAllColumnsBySql(Set<AutoTpResSet> autoTpResSets, String sql) {
        List<String> tableList = DruidParseQuerySql.parseSqlTableToList(sql);
        if (tableList.isEmpty()) {
            throw new BusinessException("该sql有误请检查");
        }
        for (String table_name : tableList) {
            List<Map<String, Object>> columns = DataTableUtil.getColumnByTableName(Dbo.db(), table_name);
            setAllColumns(autoTpResSets, table_name, columns);
        }
    }

    private void setAutoTpResSet(Set<AutoTpResSet> autoTpResSets, AutoTpResSet auto_tp_res_set, List<Map<String, Object>> columns) {
        for (Map<String, Object> column : columns) {
            if (auto_tp_res_set.getColumn_en_name().toLowerCase().equals(column.get("column_name").toString().toLowerCase())) {
                auto_tp_res_set.setColumn_cn_name(column.get("column_ch_name").toString());
                if (numbersArray.contains(column.get("data_type").toString())) {
                    auto_tp_res_set.setColumn_type(AutoValueType.ShuZhi.getCode());
                } else {
                    auto_tp_res_set.setColumn_type(AutoValueType.ZiFuChuan.getCode());
                }
                autoTpResSets.add(auto_tp_res_set);
            }
        }
    }

    private void setAllColumns(Set<AutoTpResSet> autoTpResSets, String table_name, List<Map<String, Object>> columns) {
        for (Map<String, Object> column : columns) {
            AutoTpResSet auto_tp_res_set = new AutoTpResSet();
            auto_tp_res_set.setSource_table_name(table_name);
            auto_tp_res_set.setColumn_en_name(column.get("column_name").toString());
            auto_tp_res_set.setColumn_cn_name(column.get("column_ch_name").toString());
            auto_tp_res_set.setRes_show_column(column.get("column_name").toString());
            if (numbersArray.contains(column.get("data_type").toString())) {
                auto_tp_res_set.setColumn_type(AutoValueType.ShuZhi.getCode());
            } else {
                auto_tp_res_set.setColumn_type(AutoValueType.ZiFuChuan.getCode());
            }
            autoTpResSets.add(auto_tp_res_set);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "sql", desc = "", range = "")
    @Return(desc = "", range = "")
    private Map<String, String> getTableAndAliasName(String sql) {
        String format = SQLUtils.format(sql, JdbcConstants.ORACLE);
        List<String> sqlPartList = StringUtil.split(format, "\n");
        Map<String, String> map = new HashMap<>();
        for (String sqlPart : sqlPartList) {
            if (sqlPart.startsWith("FROM")) {
                List<String> tableAlias = StringUtil.split(sqlPart, Constant.SPACE);
                if (tableAlias.size() == 3) {
                    map.put(tableAlias.get(2).toUpperCase(), tableAlias.get(1));
                } else {
                    map.put(tableAlias.get(1).toUpperCase(), tableAlias.get(1));
                }
            } else if (sqlPart.contains("JOIN")) {
                List<String> tableAlias = StringUtil.split(sqlPart, Constant.SPACE);
                map.put(tableAlias.get(3).toUpperCase(), tableAlias.get(2));
            }
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "", nullable = true)
    @Param(name = "data_layer", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchFieldById(String file_id, String data_layer) {
        return DataTableUtil.getTableInfoAndColumnInfo(data_layer, file_id);
    }
}
