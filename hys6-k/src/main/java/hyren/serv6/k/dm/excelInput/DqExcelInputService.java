package hyren.serv6.k.dm.excelInput;

import com.alibaba.excel.EasyExcel;
import com.alibaba.excel.context.AnalysisContext;
import com.alibaba.excel.event.AnalysisEventListener;
import fd.ng.core.utils.DateUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.codes.Store_type;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.fileutil.FileUploadUtil;
import hyren.serv6.commons.collection.ProcessingData;
import hyren.serv6.commons.collection.bean.LayerBean;
import hyren.serv6.commons.config.webconfig.WebinfoProperties;
import hyren.serv6.k.dm.excelInput.bean.DqDefnitionExcelVo;
import hyren.serv6.k.entity.DqDefinition;
import hyren.serv6.k.entity.DqRuleDef;
import hyren.serv6.k.utils.PrimaryKeyUtils;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;
import java.io.File;
import java.io.IOException;
import java.util.*;

@Slf4j
@Service
public class DqExcelInputService {

    private String SQL = "指定sql";

    private String TAB_NAN = "表非空";

    private String COL_FK = "字段外键检测";

    private String COL_RANG = "字段范围检测";

    private String TOTAL_SCORE = "总分校验检查";

    public void DqExcelInput(MultipartFile file) {
        File uploadFile = getUploadFile(file);
        List<DqDefinition> dqDefinitions = new ArrayList<>();
        Map<String, String> caseType = getCaseType();
        EasyExcel.read(uploadFile, DqDefnitionExcelVo.class, new AnalysisEventListener<DqDefnitionExcelVo>() {

            Integer rowNum = 1;

            @Override
            public void invoke(DqDefnitionExcelVo dqDefnitionExcelVo, AnalysisContext analysisContext) {
                getDqDefinitionExcel(dqDefnitionExcelVo, dqDefinitions, caseType, rowNum);
                rowNum++;
            }

            @Override
            public void doAfterAllAnalysed(AnalysisContext analysisContext) {
                log.info("解析Excel完成");
                saveDqDefnition(dqDefinitions);
            }
        }).sheet(0).doRead();
    }

    public void saveDqDefnition(List<DqDefinition> dqDefinitions) {
        List<Object[]> objects = new ArrayList<>();
        for (DqDefinition dqDefinition : dqDefinitions) {
            Object[] obj = new Object[33];
            obj[0] = PrimaryKeyUtils.nextId();
            obj[1] = dqDefinition.getReg_name();
            obj[2] = dqDefinition.getLoad_strategy();
            obj[3] = dqDefinition.getGroup_seq();
            obj[4] = dqDefinition.getTarget_tab();
            obj[5] = dqDefinition.getTarget_key_fields();
            obj[6] = dqDefinition.getOpposite_tab();
            obj[7] = dqDefinition.getOpposite_key_fields();
            obj[8] = dqDefinition.getRange_min_val();
            obj[9] = dqDefinition.getRange_max_val();
            obj[10] = dqDefinition.getList_vals();
            obj[11] = dqDefinition.getCheck_limit_condition();
            obj[12] = dqDefinition.getSpecify_sql();
            obj[13] = dqDefinition.getErr_data_sql();
            obj[14] = dqDefinition.getIndex_desc1();
            obj[15] = dqDefinition.getIndex_desc2();
            obj[16] = dqDefinition.getIndex_desc3();
            obj[17] = dqDefinition.getFlags();
            obj[18] = dqDefinition.getRemark();
            obj[19] = dqDefinition.getApp_updt_dt();
            obj[20] = dqDefinition.getApp_updt_ti();
            obj[21] = dqDefinition.getRule_tag();
            obj[22] = dqDefinition.getMail_receive();
            obj[23] = dqDefinition.getRule_src();
            obj[24] = dqDefinition.getIs_saveindex1();
            obj[25] = dqDefinition.getIs_saveindex2();
            obj[26] = dqDefinition.getIs_saveindex3();
            obj[27] = dqDefinition.getCase_type();
            obj[28] = dqDefinition.getUser_id();
            obj[29] = dqDefinition.getTotal_corr_fields();
            obj[30] = dqDefinition.getTotal_filter_fields();
            obj[31] = dqDefinition.getSub_group_fields();
            obj[32] = dqDefinition.getSub_filter_fields();
            objects.add(obj);
        }
        SqlOperator.Assembler sql = SqlOperator.Assembler.newInstance();
        sql.addSql("INSERT INTO dq_definition (reg_num, reg_name, load_strategy, group_seq, target_tab, target_key_fields, opposite_tab, opposite_key_fields, range_min_val, range_max_val, list_vals, check_limit_condition, specify_sql, err_data_sql, index_desc1, index_desc2, index_desc3, flags, remark, app_updt_dt, app_updt_ti, rule_tag, mail_receive, rule_src, is_saveindex1, is_saveindex2, is_saveindex3, case_type, user_id, total_corr_fields, total_filter_fields, sub_group_fields, sub_filter_fields) VALUES " + "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,? ,?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        if (objects.size() > 0) {
            SqlOperator.executeBatch(Dbo.db(), sql.sql(), objects);
        }
    }

    public Map<String, String> getCaseType() {
        List<DqRuleDef> dqRuleDefs = Dbo.queryList(DqRuleDef.class, "SELECT * FROM " + DqRuleDef.TableName);
        Map<String, String> typeMaps = new HashMap<>();
        for (DqRuleDef dqRuleDef : dqRuleDefs) {
            typeMaps.put(dqRuleDef.getCase_type_desc(), dqRuleDef.getCase_type());
        }
        return typeMaps;
    }

    public void getDqDefinitionExcel(DqDefnitionExcelVo dqDefnitionExcelVo, List<DqDefinition> dqDefinitions, Map<String, String> caseTypeMaps, Integer rowNum) {
        DqDefinition dqDefinition = new DqDefinition();
        if (StringUtil.isEmpty(dqDefnitionExcelVo.getReg_name())) {
            return;
        }
        long num = Dbo.queryNumber("select count(1) from " + DqDefinition.TableName + " where REG_NAME = ? ", dqDefnitionExcelVo.getReg_name()).orElse(0);
        if (num > 0) {
            throw new BusinessException(rowNum + "行" + dqDefnitionExcelVo.getReg_name() + "已存在!");
        }
        dqDefinition.setReg_name(dqDefnitionExcelVo.getReg_name());
        if (StringUtil.isEmpty(dqDefnitionExcelVo.getFlags())) {
            throw new BusinessException(rowNum + "行中规则级别不能为空!");
        }
        dqDefinition.setFlags(dqDefnitionExcelVo.getFlags());
        String caseType = dqDefnitionExcelVo.getCase_type();
        if (StringUtil.isEmpty(caseType)) {
            throw new BusinessException(rowNum + "行中规则类型不能为空!");
        }
        if (caseTypeMaps.get(caseType).isEmpty()) {
            throw new BusinessException(rowNum + "行中类型出错！");
        }
        dqDefinition.setCase_type(caseTypeMaps.get(caseType));
        dqDefinition.setRule_src(dqDefnitionExcelVo.getRule_src());
        dqDefinition.setLoad_strategy(dqDefnitionExcelVo.getLoad_strategy());
        dqDefinition.setGroup_seq(dqDefnitionExcelVo.getGroup_seq());
        dqDefinition.setRule_tag(dqDefnitionExcelVo.getRule_tag());
        if (!SQL.equals(caseType) && StringUtil.isEmpty(dqDefnitionExcelVo.getTarget_tab())) {
            throw new BusinessException(rowNum + "行中目标表名或总表表名不能为空!");
        }
        dqDefinition.setTarget_tab(dqDefnitionExcelVo.getTarget_tab());
        if (!SQL.equals(caseType) && !TAB_NAN.equals(caseType) && StringUtil.isEmpty(dqDefnitionExcelVo.getTarget_key_fields())) {
            throw new BusinessException(rowNum + "行中目标字段或总表校验字段不能为空!");
        }
        dqDefinition.setTarget_key_fields(dqDefnitionExcelVo.getTarget_key_fields());
        String oppositeTab = dqDefnitionExcelVo.getOpposite_tab();
        if (COL_FK.equals(caseType) || TOTAL_SCORE.equals(caseType)) {
            if (StringUtil.isEmpty(oppositeTab)) {
                throw new BusinessException(rowNum + "行中比对表名或分表表名不能为空!");
            }
            dqDefinition.setOpposite_tab(oppositeTab);
        }
        String oppositeKeyFields = dqDefnitionExcelVo.getOpposite_key_fields();
        if (COL_FK.equals(caseType) || TOTAL_SCORE.equals(caseType)) {
            if (StringUtil.isEmpty(oppositeKeyFields)) {
                throw new BusinessException(rowNum + "行中比对字段或分表校验字段不能为空!");
            }
            dqDefinition.setOpposite_key_fields(oppositeKeyFields);
        }
        String rangeMinVal = dqDefnitionExcelVo.getRange_min_val();
        if (COL_RANG.equals(caseType)) {
            if (StringUtil.isEmpty(rangeMinVal)) {
                throw new BusinessException(rowNum + "行中范围最小值不能为空!");
            }
            dqDefinition.setRange_min_val(rangeMinVal);
        }
        String rangeMaxVal = dqDefnitionExcelVo.getRange_max_val();
        if (COL_RANG.equals(caseType)) {
            if (StringUtil.isEmpty(rangeMaxVal)) {
                throw new BusinessException(rowNum + "行中范围最大值不能为空!");
            }
            dqDefinition.setRange_max_val(rangeMaxVal);
        }
        dqDefinition.setList_vals(dqDefnitionExcelVo.getList_vals());
        dqDefinition.setTotal_filter_fields(dqDefnitionExcelVo.getTotal_filter_fields());
        if (TOTAL_SCORE.equals(caseType)) {
            if (StringUtil.isEmpty(dqDefnitionExcelVo.getTotal_corr_fields())) {
                throw new BusinessException(rowNum + "行中总表关联字段不能为空!");
            }
            dqDefinition.setTotal_corr_fields(dqDefnitionExcelVo.getTotal_corr_fields());
            if (StringUtil.isEmpty(dqDefnitionExcelVo.getSub_group_fields())) {
                throw new BusinessException(rowNum + "行中分表分组字段不能为空!");
            }
            dqDefinition.setSub_group_fields(dqDefnitionExcelVo.getSub_group_fields());
        }
        dqDefinition.setSub_filter_fields(dqDefnitionExcelVo.getSub_filter_condition());
        dqDefinition.setRemark(dqDefnitionExcelVo.getRemark());
        LayerBean layerBean = null;
        try {
            layerBean = ProcessingData.getLayerByTable(dqDefinition.getTarget_tab(), Dbo.db()).get(0);
        } catch (Exception e) {
            layerBean = null;
        }
        setSql(caseType, dqDefinition, layerBean);
        if (StringUtil.isBlank(dqDefinition.getIs_saveindex1())) {
            dqDefinition.setIs_saveindex1(IsFlag.Fou.getCode());
        }
        if (StringUtil.isBlank(dqDefinition.getIs_saveindex2())) {
            dqDefinition.setIs_saveindex2(IsFlag.Fou.getCode());
        }
        if (StringUtil.isBlank(dqDefinition.getIs_saveindex3())) {
            dqDefinition.setIs_saveindex3(IsFlag.Fou.getCode());
        }
        dqDefinition.setUser_id(UserUtil.getUserId());
        dqDefinition.setApp_updt_dt(DateUtil.getSysDate());
        dqDefinition.setApp_updt_ti(DateUtil.getSysTime());
        dqDefinitions.add(dqDefinition);
    }

    public void setSql(String type, DqDefinition dqDefinition, LayerBean layerBean) {
        if (layerBean == null) {
            return;
        }
        String database_type = layerBean.getLayerAttr().get("database_type");
        if (database_type.isEmpty()) {
            return;
        }
        StringBuilder specify_sql = new StringBuilder("");
        StringBuilder err_data_sql = new StringBuilder("");
        if (StringUtil.isEmpty(dqDefinition.getTarget_tab())) {
            throw new BusinessException("目标表名不能为空");
        }
        switch(type) {
            case "字段枚举检测":
                specify_sql.append("SELECT COUNT(1) AS index1 FROM  " + dqDefinition.getTarget_tab());
                specify_sql.append(" T1  WHERE  ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                if (StringUtil.isEmpty(dqDefinition.getList_vals())) {
                    throw new BusinessException("清单值域不能为空!");
                }
                String[] split = dqDefinition.getTarget_key_fields().split(",");
                for (int i = 0; i < split.length; i++) {
                    if (i != 0) {
                        specify_sql.append(" AND ");
                    }
                    specify_sql.append(split[i].toUpperCase() + " NOT IN ( " + dqDefinition.getList_vals() + " ) ");
                }
                specify_sql.append(";");
                specify_sql.append(" SELECT  COUNT(1) AS index2 FROM " + dqDefinition.getTarget_tab());
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append(" WHERE (" + dqDefinition.getCheck_limit_condition() + ") ; ");
                } else {
                    specify_sql.append(";");
                }
                err_data_sql.append("SELECT " + dqDefinition.getTarget_key_fields() + " FROM " + dqDefinition.getTarget_key_fields() + " T1 WHERE");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                String[] errsplit = dqDefinition.getTarget_key_fields().split(",");
                for (int i = 0; i < errsplit.length; i++) {
                    if (i != 0) {
                        specify_sql.append(" AND ");
                    }
                    err_data_sql.append(errsplit[i].toUpperCase() + " NOT IN ( " + dqDefinition.getList_vals() + " ) ");
                }
                break;
            case "字段外键检测":
                specify_sql.append("SELECT COUNT(1) AS index1 FROM " + dqDefinition.getTarget_tab() + " T1 WHERE  ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                if (StringUtil.isEmpty(dqDefinition.getOpposite_tab())) {
                    throw new BusinessException("比对表名不能为空!");
                }
                if (StringUtil.isEmpty(dqDefinition.getOpposite_key_fields())) {
                    throw new BusinessException("比对字段不能为空!");
                }
                String oppositSql = " SELECT " + dqDefinition.getOpposite_key_fields() + " FROM " + dqDefinition.getOpposite_tab() + " T2 ";
                String[] split1 = dqDefinition.getTarget_key_fields().split(",");
                for (int i = 0; i < split1.length; i++) {
                    if (i != 0) {
                        specify_sql.append(" AND ");
                    }
                    specify_sql.append(split1[i].toUpperCase() + " NOT IN ( " + oppositSql + " ) ");
                }
                specify_sql.append(";");
                specify_sql.append("SELECT COUNT(1) AS index1 FROM " + dqDefinition.getTarget_tab() + " T1 ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append(" WHERE (" + dqDefinition.getCheck_limit_condition() + ") ; ");
                } else {
                    specify_sql.append(";");
                }
                err_data_sql.append("SELECT " + dqDefinition.getTarget_key_fields() + " FROM " + dqDefinition.getTarget_tab() + " T1 WHERE ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    err_data_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                for (int i = 0; i < split1.length; i++) {
                    if (i != 0) {
                        specify_sql.append(" AND ");
                    }
                    specify_sql.append(split1[i].toUpperCase() + " NOT IN ( " + oppositSql + " ) ");
                }
                break;
            case "字段主键检测":
                String cloPkSql = "SELECT " + dqDefinition.getTarget_key_fields() + " FROM " + dqDefinition.getTarget_tab() + " GROUP BY " + dqDefinition.getTarget_key_fields() + " HAVING COUNT(1) > 1  ";
                specify_sql.append("SELECT COUNT(1) AS index1  FROM (" + cloPkSql + ") T ;");
                specify_sql.append("SELECT COUNT(1) AS index2 FROM " + dqDefinition.getTarget_tab() + " T;");
                err_data_sql.append("SELECT " + dqDefinition.getTarget_key_fields() + " FROM (" + cloPkSql + ") T");
                break;
            case "字段范围检测":
                specify_sql.append("SELECT  COUNT(1) AS index1 FROM " + dqDefinition.getTarget_tab() + " WHERE ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                String[] split2 = dqDefinition.getTarget_key_fields().split(",");
                if (StringUtil.isEmpty(dqDefinition.getRange_max_val())) {
                    throw new BusinessException("范围最大值不能为空");
                }
                if (StringUtil.isEmpty(dqDefinition.getRange_min_val())) {
                    throw new BusinessException("范围最小值不能为空");
                }
                for (int i = 0; i < split2.length; i++) {
                    if (i != 0) {
                        specify_sql.append(" AND ");
                    }
                    specify_sql.append(" ( " + split2[i] + " <= " + dqDefinition.getRange_min_val() + " OR " + split2[i] + " >= " + dqDefinition.getRange_max_val() + " ) ");
                }
                break;
            case "字段正则表达式":
                specify_sql.append("SELECT  COUNT(1) AS index1 FROM " + dqDefinition.getTarget_tab() + " T1 WHERE ");
                if (!StringUtil.isEmpty(dqDefinition.getTotal_filter_fields())) {
                    specify_sql.append("  (" + dqDefinition.getTotal_filter_fields() + ") AND  ");
                }
                String[] split4 = dqDefinition.getTarget_key_fields().split(",");
                if (Store_type.DATABASE.getCode().equals(layerBean.getStore_type())) {
                    if ("POSTGRESQL".contains(database_type)) {
                        for (int i = 0; i < split4.length; i++) {
                            if (i != 0) {
                                specify_sql.append(" AND ");
                            }
                            specify_sql.append(" NOT( regexp_match( CAST(" + split4[i] + "," + dqDefinition.getList_vals() + ")) ");
                        }
                    } else if ("ORACLE10G".contains(database_type) || "ORACLE9I".contains(database_type)) {
                        for (int i = 0; i < split4.length; i++) {
                            if (i != 0) {
                                specify_sql.append(" AND ");
                            }
                            specify_sql.append("  NOT( regexp_like ( " + split4[i] + "," + dqDefinition.getList_vals() + ")) ");
                        }
                    } else {
                        for (int i = 0; i < split4.length; i++) {
                            if (i != 0) {
                                specify_sql.append(" AND ");
                            }
                            specify_sql.append("  NOT(( " + split4[i] + ") ~ (" + dqDefinition.getList_vals() + ")) ");
                        }
                    }
                } else if (Store_type.HIVE.getCode().equals(layerBean.getStore_type()) || Store_type.HBASE.getCode().equals(layerBean.getStore_type())) {
                    for (int i = 0; i < split4.length; i++) {
                        if (i != 0) {
                            specify_sql.append(" AND ");
                        }
                        specify_sql.append("  NOT(( " + split4[i] + ") regexp (" + dqDefinition.getList_vals() + ")) ");
                    }
                } else {
                    for (int i = 0; i < split4.length; i++) {
                        if (i != 0) {
                            specify_sql.append(" AND ");
                        }
                        specify_sql.append("  NOT(( " + split4[i] + ") ~ (" + dqDefinition.getList_vals() + ")) ");
                    }
                }
                err_data_sql.append("SELECT " + dqDefinition.getTarget_key_fields() + " FROM " + dqDefinition.getTarget_tab() + " WHERE ");
                if (!StringUtil.isEmpty(dqDefinition.getTotal_filter_fields())) {
                    specify_sql.append("  (" + dqDefinition.getTotal_filter_fields() + ") AND  ");
                }
                if (Store_type.DATABASE.getCode().equals(layerBean.getStore_type())) {
                    if ("POSTGRESQL".contains(database_type)) {
                        for (int i = 0; i < split4.length; i++) {
                            if (i != 0) {
                                specify_sql.append(" AND ");
                            }
                            specify_sql.append(" NOT( regexp_match( CAST(" + split4[i] + "," + dqDefinition.getList_vals() + ")) ");
                        }
                    } else if ("ORACLE10G".contains(database_type) || "ORACLE9I".contains(database_type)) {
                        for (int i = 0; i < split4.length; i++) {
                            if (i != 0) {
                                specify_sql.append(" AND ");
                            }
                            specify_sql.append("  NOT( regexp_like ( " + split4[i] + "," + dqDefinition.getList_vals() + ")) ");
                        }
                    } else {
                        for (int i = 0; i < split4.length; i++) {
                            if (i != 0) {
                                specify_sql.append(" AND ");
                            }
                            specify_sql.append("  NOT(( " + split4[i] + ") ~ (" + dqDefinition.getList_vals() + ")) ");
                        }
                    }
                } else if (Store_type.HIVE.getCode().equals(layerBean.getStore_type()) || Store_type.HBASE.getCode().equals(layerBean.getStore_type())) {
                    for (int i = 0; i < split4.length; i++) {
                        if (i != 0) {
                            specify_sql.append(" AND ");
                        }
                        specify_sql.append("  NOT(( " + split4[i] + ") regexp (" + dqDefinition.getList_vals() + ")) ");
                    }
                } else {
                    for (int i = 0; i < split4.length; i++) {
                        if (i != 0) {
                            specify_sql.append(" AND ");
                        }
                        specify_sql.append("  NOT(( " + split4[i] + ") ~ (" + dqDefinition.getList_vals() + ")) ");
                    }
                }
                break;
            case "指定sql":
                return;
            case "表非空":
                specify_sql.append("SELECT COUNT(1) AS index1 FROM " + dqDefinition.getTarget_tab() + " ;");
                break;
            case "总分校验检查":
                StringBuilder sumSQL = new StringBuilder();
                sumSQL.append(" SELECT " + dqDefinition.getTotal_corr_fields() + "," + dqDefinition.getTarget_key_fields() + " FROM " + dqDefinition.getTarget_tab());
                if (!StringUtil.isEmpty(dqDefinition.getTotal_filter_fields())) {
                    sumSQL.append(" WHERE (" + dqDefinition.getTotal_filter_fields() + ")  ");
                }
                if (StringUtil.isEmpty(dqDefinition.getSub_group_fields())) {
                    throw new BusinessException("分表分组字段不能为空");
                }
                if (StringUtil.isEmpty(dqDefinition.getOpposite_tab())) {
                    throw new BusinessException("分表字段不能为空");
                }
                StringBuilder subSQL = new StringBuilder();
                subSQL.append("SELECT " + dqDefinition.getSub_group_fields() + ",sum(" + dqDefinition.getOpposite_key_fields() + ") sum_" + dqDefinition.getOpposite_key_fields() + " FROM " + dqDefinition.getOpposite_tab());
                if (!StringUtil.isEmpty(dqDefinition.getSub_filter_fields())) {
                    subSQL.append(" WHERE " + dqDefinition.getSub_filter_fields());
                }
                subSQL.append(" GROUP BY " + dqDefinition.getSub_group_fields());
                specify_sql.append("SELECT count(1) index1  FROM (" + sumSQL + ") T1 JOIN ( " + subSQL + " ) T2 " + " ON T1." + dqDefinition.getTotal_corr_fields() + " = T2." + dqDefinition.getSub_group_fields() + " WHERE T1." + dqDefinition.getTarget_key_fields() + " <>  sum_" + dqDefinition.getOpposite_key_fields() + ";");
                specify_sql.append(" SELECT  COUNT(1) index2 FROM " + sumSQL + " ) T1 JOIN (" + subSQL + " ) T2 ON T1." + dqDefinition.getTotal_corr_fields() + " = T2." + dqDefinition.getSub_group_fields());
                err_data_sql.append("SELETC T1." + dqDefinition.getTarget_key_fields() + ",T1." + dqDefinition.getTotal_corr_fields() + " sum_" + dqDefinition.getOpposite_key_fields() + " FROM (" + sumSQL + ") T1 JOIN (" + subSQL + ") T2 ON T1." + dqDefinition.getTotal_corr_fields() + " = T2." + dqDefinition.getSub_group_fields() + " WHERE T1." + dqDefinition.getTarget_key_fields() + " <>  sum_" + dqDefinition.getOpposite_key_fields());
                break;
            case "字段非空":
                specify_sql.append("SELECT COUNT(1) AS index1 FROM  " + dqDefinition.getTarget_tab() + " WHERE ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    specify_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                String[] split3 = dqDefinition.getTarget_key_fields().split(",");
                if ("SQLSERVER".contains(database_type)) {
                    for (int i = 0; i < split3.length; i++) {
                        if (i != 0) {
                            err_data_sql.append(" AND ");
                        }
                        err_data_sql.append(" ( " + split3[i] + " IS NULL OR LTRIM( RTRIM(" + split3[i] + "))= '' ) ");
                    }
                } else {
                    for (int i = 0; i < split3.length; i++) {
                        if (i != 0) {
                            err_data_sql.append(" AND ");
                        }
                        err_data_sql.append(" ( " + split3[i] + " IS NULL OR TRIM(" + split3[i] + ")= '' ) ");
                    }
                }
                specify_sql.append("; ");
                specify_sql.append(" SELECT  COUNT(1) AS index2  FROM " + dqDefinition.getTarget_tab() + " ;");
                err_data_sql.append(" SELECT * FROM " + dqDefinition.getTarget_tab() + " WHERE ");
                if (!StringUtil.isEmpty(dqDefinition.getCheck_limit_condition())) {
                    err_data_sql.append("(" + dqDefinition.getCheck_limit_condition() + ") AND ");
                }
                if ("SQLSERVER".contains(database_type)) {
                    for (int i = 0; i < split3.length; i++) {
                        if (i != 0) {
                            err_data_sql.append(" AND ");
                        }
                        err_data_sql.append(" ( " + split3[i] + " IS NULL OR LTRIM( RTRIM(" + split3[i] + "))= '' ) ");
                    }
                } else {
                    for (int i = 0; i < split3.length; i++) {
                        if (i != 0) {
                            err_data_sql.append(" AND ");
                        }
                        err_data_sql.append(" ( " + split3[i] + " IS NULL OR TRIM(" + split3[i] + ")= '' ) ");
                    }
                }
                break;
            default:
                throw new BusinessException("规则类型未找到");
        }
        dqDefinition.setSpecify_sql(specify_sql.toString());
        dqDefinition.setErr_data_sql(err_data_sql.toString());
    }

    private File getUploadFile(MultipartFile file) {
        File destFileDir = new File(WebinfoProperties.FileUpload_SavedDirName);
        if (!destFileDir.exists() && !destFileDir.isDirectory()) {
            if (!destFileDir.mkdirs()) {
                throw new BusinessException("创建文件目录失败");
            }
        }
        String originalFileName = file.getOriginalFilename();
        String pathname = destFileDir.getPath() + File.separator + originalFileName;
        File destFile = new File(pathname);
        try {
            file.transferTo(destFile.toPath());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        File excelFile = FileUploadUtil.getUploadedFile(pathname);
        if (!excelFile.exists()) {
            throw new BusinessException("excel文件不存在!");
        }
        return excelFile;
    }
}
