package hyren.serv6.h.manage_version;

import com.alibaba.druid.sql.ast.statement.SQLSelectItem;
import fd.ng.core.utils.Validator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.DataSourceType;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.datatree.background.query.DMLDataQuery;
import hyren.serv6.base.datatree.background.query.TreeDataQuery;
import hyren.serv6.base.datatree.background.utils.DataConvertedNodeData;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.*;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.base.utils.DruidParseQuerySql;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.h.market.dmmoduletable.DmModuleTableService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class MarketVersionManageService {

    public Map<String, List<Map<String, Map<String, Object>>>> getDataTableMappingInfos(Long database_id, List<String> version_date_s) {
        Validator.notNull(database_id, "模型表不可为空");
        Validator.notEmpty(version_date_s, "版本日期列表不可为空");
        List<DmJobTableInfo> jobtabIdList = Dbo.queryList(Dbo.db(), DmJobTableInfo.class, "select * from " + DmJobTableInfo.TableName + " where module_table_id = ? ", database_id);
        List<String> newVersionList = version_date_s.stream().filter(v -> !v.equals(Constant._MAX_DATE_8)).collect(Collectors.toList());
        boolean hasMaxDate = newVersionList.size() != version_date_s.size();
        Map<String, List<Map<String, Map<String, Object>>>> retMap = new HashMap<String, List<Map<String, Map<String, Object>>>>();
        for (int i = 0; i < jobtabIdList.size(); i++) {
            Map<String, Map<String, Object>> jobSqlMap = new HashMap<String, Map<String, Object>>();
            Map<String, String> versionSqlMap = new HashMap<String, String>();
            if (hasMaxDate) {
                Map<String, Object> maxJobSql = new HashMap<String, Object>();
                Validator.notEmpty(jobtabIdList.get(i).getJobtab_execute_sql(), "未找到" + jobtabIdList.get(i).getJobtab_en_name() + "当前执行sql");
                versionSqlMap.put(Constant._MAX_DATE_8, jobtabIdList.get(i).getJobtab_execute_sql());
            }
            for (String version : newVersionList) {
                DmJobTableVersionInfo jobV = Dbo.queryOneObject(Dbo.db(), DmJobTableVersionInfo.class, "select * from " + DmJobTableVersionInfo.TableName + " where jobtab_id = ? and version_date = ?", jobtabIdList.get(i).getJobtab_id(), version).orElse(null);
                if (jobV != null) {
                    versionSqlMap.put(jobV.getVersion_date(), jobV.getJobtab_execute_sql());
                }
            }
            for (Map.Entry<String, String> entry : versionSqlMap.entrySet()) {
                Map<String, Object> map = new HashMap<String, Object>();
                map.put("execute_sql", entry.getValue());
                DruidParseQuerySql sql_parse_info = new DruidParseQuerySql(entry.getValue());
                List<String> table_name_s = DruidParseQuerySql.parseSqlTableToList(entry.getValue());
                List<Object> highlight_list = new ArrayList<>(table_name_s);
                if (sql_parse_info.selectList != null) {
                    highlight_list.addAll(sql_parse_info.selectList.stream().map(SQLSelectItem::toString).collect(Collectors.toList()));
                }
                map.put("highlight_list", highlight_list);
                jobSqlMap.put(entry.getKey(), map);
            }
            List<Map<String, Map<String, Object>>> list = new ArrayList<>();
            list.add(jobSqlMap);
            retMap.put(jobtabIdList.get(i).getJobtab_en_name() + "_" + (i + 1), list);
        }
        return retMap;
    }

    @Autowired
    DmModuleTableService dmModuleTableService;

    @Deprecated
    public Map<String, Object> getDataTableStructureInfos(long jobtab_id, List<String> version_date_s) {
        Validator.notBlank(String.valueOf(jobtab_id));
        if (null == version_date_s || version_date_s.size() == 0) {
            throw new BusinessException("版本日期列表不能为空!");
        }
        Map<String, List<Map<String, Object>>> datatableFieldInfos_s_map = new HashMap<>();
        Set<String> field_en_nam_s = new HashSet<>();
        for (String version_date : version_date_s) {
            List<Map<String, Object>> fieldMapList = new ArrayList<>();
            if (!version_date.equals(Constant._MAX_DATE_8)) {
                fieldMapList = Dbo.queryList("select jobtab_field_en_name as field_en_name, jobtab_field_cn_name as field_cn_name," + "jobtab_field_type as field_type from " + DmJobTableFieldVersionInfo.TableName + " where jobtab_id = ? " + "and version_date = ? ", jobtab_id, version_date);
                if (fieldMapList.isEmpty()) {
                    fieldMapList = Dbo.queryList("select field_en_name,field_cn_name,field_type from " + DmModuleTableFieldVersion.TableName + " where module_table_id = ? " + "and version_date = ? ", jobtab_id, version_date);
                }
            } else {
                fieldMapList = Dbo.queryList("select jobtab_field_en_name as field_en_name, jobtab_field_cn_name as field_cn_name," + "jobtab_field_type as field_type from " + DmJobTableFieldInfo.TableName + " where jobtab_id = ? ", jobtab_id);
                if (fieldMapList.isEmpty()) {
                    fieldMapList = Dbo.queryList("select field_en_name,field_cn_name,field_type from " + DmModuleTableFieldInfo.TableName + " where module_table_id = ?", jobtab_id);
                }
            }
            if (fieldMapList.isEmpty()) {
                return null;
            }
            datatableFieldInfos_s_map.put(version_date, new ArrayList<>(fieldMapList));
            fieldMapList.forEach(datatable_field_info -> field_en_nam_s.add(datatable_field_info.get("field_en_name").toString()));
        }
        Map<String, Object> dataTableStructureInfoMap = new HashMap<>();
        datatableFieldInfos_s_map.forEach((version_date, selectedVersionFieldList) -> {
            List<Map<String, Map<String, String>>> dfi_map_s = new ArrayList<>();
            field_en_nam_s.forEach(field_en_name -> {
                Map<String, Map<String, String>> dfi_map = new HashMap<>();
                Map<String, String> field_en_name_map = new HashMap<>();
                field_en_name_map.put("field_en_name", "-");
                field_en_name_map.put("is_same", IsFlag.Fou.getCode());
                Map<String, String> field_cn_name_map = new HashMap<>();
                field_cn_name_map.put("field_cn_name", "-");
                field_cn_name_map.put("is_same", IsFlag.Fou.getCode());
                Map<String, String> field_type_map = new HashMap<>();
                field_type_map.put("field_type", "-");
                field_type_map.put("is_same", IsFlag.Fou.getCode());
                for (Map<String, Object> datatable_field_info : selectedVersionFieldList) {
                    String enName = datatable_field_info.get("field_en_name").toString();
                    String cnName = datatable_field_info.get("field_cn_name").toString();
                    String fieldType = datatable_field_info.get("field_type").toString();
                    if (field_en_name.equalsIgnoreCase(enName)) {
                        field_en_name_map.put("field_en_name", enName);
                        field_cn_name_map.put("field_cn_name", cnName);
                        field_type_map.put("field_type", fieldType);
                    }
                    if (enName.equalsIgnoreCase(field_en_name_map.get("field_en_name"))) {
                        field_en_name_map.put("is_same", IsFlag.Shi.getCode());
                    }
                    if (cnName.equalsIgnoreCase(field_cn_name_map.get("field_cn_name"))) {
                        field_cn_name_map.put("is_same", IsFlag.Shi.getCode());
                    }
                    if (fieldType.equalsIgnoreCase(field_type_map.get("field_type"))) {
                        field_type_map.put("is_same", IsFlag.Shi.getCode());
                    }
                }
                dfi_map.put("field_en_name_map", field_en_name_map);
                dfi_map.put("field_cn_name_map", field_cn_name_map);
                dfi_map.put("field_type_map", field_type_map);
                dfi_map_s.add(dfi_map);
            });
            dataTableStructureInfoMap.put(version_date, dfi_map_s);
        });
        return dataTableStructureInfoMap;
    }

    public List<Node> getMarketVerManageTreeData() {
        List<Map<String, Object>> dataList = new ArrayList<>(TreeDataQuery.getSourceTreeInfos(TreePageSource.MARKET_VERSION_MANAGE));
        List<Map<String, Object>> dmlDataInfos = DMLDataQuery.getDMLDataInfos(UserUtil.getUser());
        dataList.addAll(DataConvertedNodeData.conversionDMLDataInfos(dmlDataInfos));
        for (Map<String, Object> dmlDataInfo : dmlDataInfos) {
            long data_mart_id = (long) dmlDataInfo.get("data_mart_id");
            List<Map<String, Object>> dmlCategoryInfos = DMLDataQuery.getDMLCategoryInfos(data_mart_id);
            if (!dmlCategoryInfos.isEmpty()) {
                for (Map<String, Object> dmlCategoryInfo : dmlCategoryInfos) {
                    dataList.add(DataConvertedNodeData.conversionDMLCategoryInfos(dmlCategoryInfo));
                    long category_id = (long) dmlCategoryInfo.get("category_id");
                    List<Map<String, Object>> dmlTableInfos = DMLDataQuery.getDMLTableInfos(category_id, null);
                    if (!dmlTableInfos.isEmpty()) {
                        dataList.addAll(DataConvertedNodeData.conversionDMLTableInfos(dmlTableInfos));
                        for (Map<String, Object> dmlTableInfo : dmlTableInfos) {
                            long module_table_id = (long) dmlTableInfo.get("module_table_id");
                            List<DmModuleTableVersion> dmModuleTableVersions = Dbo.queryList(DmModuleTableVersion.class, " select * from " + DmModuleTableVersion.TableName + " where module_table_id = ?", module_table_id);
                            DmModuleTableVersion dmModuleTable = Dbo.queryOneObject(DmModuleTableVersion.class, " select * from " + DmModuleTable.TableName + " where module_table_id = ?", module_table_id).orElseThrow(() -> new BusinessException(" data illegal ..."));
                            dmModuleTable.setVersion_date(Constant._MAX_DATE_8);
                            dmModuleTable.setMtab_ver_id(PrimayKeyGener.getNextId());
                            if (!dmModuleTableVersions.isEmpty()) {
                                dmModuleTableVersions.add(dmModuleTable);
                            }
                            for (DmModuleTableVersion moduleTableVersion : dmModuleTableVersions) {
                                List<Map<String, Object>> dmlTableVersionNodes = new ArrayList<>();
                                Map<String, Object> map = new HashMap<>();
                                map.put("id", moduleTableVersion.getModule_table_id() + "_" + moduleTableVersion.getVersion_date());
                                map.put("label", moduleTableVersion.getVersion_date());
                                map.put("parent_id", moduleTableVersion.getModule_table_id());
                                map.put("classify_id", moduleTableVersion.getModule_table_id());
                                map.put("file_id", moduleTableVersion.getModule_table_id());
                                map.put("table_name", moduleTableVersion.getVersion_date());
                                map.put("hyren_name", moduleTableVersion.getVersion_date());
                                map.put("original_name", moduleTableVersion.getVersion_date());
                                map.put("data_layer", DataSourceType.DML.getCode());
                                map.put("tree_page_source", TreePageSource.MARKET_VERSION_MANAGE);
                                map.put("description", "sql:" + moduleTableVersion.getRemark());
                                dmlTableVersionNodes.add(map);
                                dataList.addAll(dmlTableVersionNodes);
                            }
                        }
                    }
                }
            }
        }
        List<Node> dataConversionTreeInfo = NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
        return dataConversionTreeInfo;
    }

    private void treeAddTasksAndIn(List<Map<String, Object>> dataList, long module_table_id) {
        List<Map<String, Object>> dmTaskInfNodes = new ArrayList<>();
        List<DmTaskInfo> dmTaskInfos = Dbo.queryList(DmTaskInfo.class, " select * from " + DmTaskInfo.TableName + " where module_table_id = ?", module_table_id);
        for (DmTaskInfo dmTaskInfo : dmTaskInfos) {
            Map<String, Object> taskMap = new HashMap<>();
            taskMap.put("id", dmTaskInfo.getTask_id());
            taskMap.put("label", dmTaskInfo.getTask_name());
            taskMap.put("parent_id", dmTaskInfo.getModule_table_id());
            taskMap.put("classify_id", dmTaskInfo.getModule_table_id());
            taskMap.put("file_id", dmTaskInfo.getTask_id());
            taskMap.put("table_name", dmTaskInfo.getTask_name());
            taskMap.put("hyren_name", dmTaskInfo.getTask_name());
            taskMap.put("original_name", dmTaskInfo.getTask_name());
            taskMap.put("data_layer", DataSourceType.DML.getCode());
            taskMap.put("description", "任务名：" + dmTaskInfo.getTask_name());
            dmTaskInfNodes.add(taskMap);
            List<DmJobTableInfo> dmJobTableInfos = Dbo.queryList(DmJobTableInfo.class, " select * from " + DmJobTableInfo.TableName + " where module_table_id = ? and task_id = ? and jobtab_is_temp = ?", module_table_id, dmTaskInfo.getTask_id(), IsFlag.Fou.getCode());
            List<Map<String, Object>> dmJobTableInfoNodes = new ArrayList<>();
            for (DmJobTableInfo dmJobTableInfo : dmJobTableInfos) {
                Map<String, Object> jobMap = new HashMap<>();
                jobMap.put("id", dmJobTableInfo.getJobtab_id());
                jobMap.put("label", dmJobTableInfo.getJobtab_en_name());
                jobMap.put("parent_id", dmJobTableInfo.getTask_id());
                jobMap.put("classify_id", dmJobTableInfo.getTask_id());
                jobMap.put("file_id", dmJobTableInfo.getJobtab_id());
                jobMap.put("table_name", dmJobTableInfo.getJobtab_en_name());
                jobMap.put("hyren_name", dmJobTableInfo.getJobtab_en_name());
                jobMap.put("original_name", dmJobTableInfo.getJobtab_en_name() + "_" + dmJobTableInfo.getJobtab_step_number());
                jobMap.put("data_layer", DataSourceType.DML.getCode());
                jobMap.put("description", "表英文名：" + dmJobTableInfo.getJobtab_en_name());
                dmJobTableInfoNodes.add(jobMap);
                List<DmJobTableVersionInfo> dmJobTableVersionInfos = Dbo.queryList(DmJobTableVersionInfo.class, " select version_date from " + DmJobTableVersionInfo.TableName + " where jobtab_id = ?", dmJobTableInfo.getJobtab_id());
                DmJobTableVersionInfo dmJobTableVersionInfo = Dbo.queryOneObject(DmJobTableVersionInfo.class, " select * from " + DmJobTableInfo.TableName + " where jobtab_id = ?", dmJobTableInfo.getJobtab_id()).orElseThrow(() -> new BusinessException(" data illegal ..."));
                dmJobTableVersionInfo.setVersion_date(Constant._MAX_DATE_8);
                dmJobTableVersionInfo.setJobtab_version_id(PrimayKeyGener.getNextId());
                if (!dmJobTableVersionInfos.isEmpty()) {
                    dmJobTableVersionInfos.add(dmJobTableVersionInfo);
                }
                if (!dmJobTableVersionInfos.isEmpty()) {
                    dataList.addAll(conversionDMLTableVersionInfos(dmJobTableInfo, dmJobTableVersionInfos));
                }
            }
            dataList.addAll(dmJobTableInfoNodes);
        }
        dataList.addAll(dmTaskInfNodes);
    }

    private static List<Map<String, Object>> conversionDMLTableVersionInfos(DmJobTableInfo dmJobTableInfo, List<DmJobTableVersionInfo> dmlTableVersionInfos) {
        List<Map<String, Object>> dmlTableVersionNodes = new ArrayList<>();
        dmlTableVersionInfos.forEach(dmlTableVersionInfo -> {
            Map<String, Object> map = new HashMap<>();
            map.put("id", dmJobTableInfo.getJobtab_id() + "_" + dmlTableVersionInfo.getVersion_date());
            map.put("label", dmlTableVersionInfo.getVersion_date());
            map.put("parent_id", dmJobTableInfo.getJobtab_id());
            map.put("classify_id", dmJobTableInfo.getJobtab_id());
            map.put("file_id", dmJobTableInfo.getJobtab_id());
            map.put("table_name", dmJobTableInfo.getJobtab_en_name());
            map.put("hyren_name", dmJobTableInfo.getJobtab_en_name());
            map.put("original_name", dmJobTableInfo.getJobtab_en_name());
            map.put("data_layer", DataSourceType.DML.getCode());
            map.put("tree_page_source", TreePageSource.MARKET_VERSION_MANAGE);
            map.put("description", "" + "表英文名：" + dmJobTableInfo.getJobtab_en_name() + "\n" + "表中文名：" + dmJobTableInfo.getJobtab_cn_name() + "\n" + "版本日期：" + dmlTableVersionInfo.getVersion_date() + "\n" + "表描述：" + dmJobTableInfo.getJobtab_remark());
            dmlTableVersionNodes.add(map);
        });
        return dmlTableVersionNodes;
    }
}
