package hyren.serv6.q.dataLineage;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.Validator;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.entity.DmJobTableInfo;
import hyren.serv6.base.entity.DmModuleTable;
import hyren.serv6.base.exception.BusinessException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import java.util.*;

@Service
@Slf4j
public class DataLineageServiceImpl {

    public List<String> fuzzySearchTableName() {
        StringBuilder sqlSb = new StringBuilder();
        sqlSb.append("select table_name from (");
        sqlSb.append("select jobtab_en_name as table_name from " + DmJobTableInfo.TableName + " group by jobtab_en_name) T ");
        sqlSb.append("union ");
        sqlSb.append("select table_name from (");
        sqlSb.append(" select module_table_en_name as table_name from " + DmModuleTable.TableName + " group by " + "module_table_en_name ) aa ");
        return Dbo.queryOneColumnList(sqlSb.toString());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "search_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> influencesDataInfo(DatabaseWrapper db, String table_name, String search_type) {
        Validator.notBlank(search_type, "搜索关系为空!");
        List<Map<String, Object>> influencesResult = new ArrayList<>();
        IsFlag is_st = IsFlag.ofEnumByCode(search_type);
        if (is_st == IsFlag.Fou) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT dds.own_source_table_name AS source_table_name,djti.jobtab_en_name AS target_table_name ");
            sb.append("FROM dm_datatable_source dds JOIN dm_job_table_info djti ON dds.jobtab_id = djti.jobtab_id ");
            sb.append(" WHERE LOWER(dds.own_source_table_name) = LOWER('" + table_name + "')" + "order by dds.own_source_table_name,djti.jobtab_en_name");
            List<Map<String, Object>> influences_table_s = SqlOperator.queryList(db, sb.toString());
            if (!influences_table_s.isEmpty()) {
                Set<String> set = new HashSet<>();
                influences_table_s.forEach(influences_data -> {
                    String tableName = influences_data.get("target_table_name").toString();
                    if (!set.contains(tableName)) {
                        Map<String, Object> map = new HashMap<>();
                        set.add(tableName);
                        map.put("name", tableName);
                        map.put("children", nextData(db, tableName));
                        influencesResult.add(map);
                    }
                });
            }
        } else if (is_st == IsFlag.Shi) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT dds.own_source_table_name AS source_table_name,dosf.field_name AS source_column_name ");
            sb.append("FROM dm_datatable_source dds JOIN dm_own_source_field dosf ON dds.own_source_table_id = dosf.own_source_table_id ");
            sb.append("WHERE  LOWER(dds.own_source_table_name) = LOWER('" + table_name + "') ORDER BY dds.own_source_table_name");
            List<Map<String, Object>> influences_column_s = SqlOperator.queryList(db, sb.toString());
            if (!influences_column_s.isEmpty()) {
                Set<String> set = new HashSet<>();
                for (Map<String, Object> influencesColumn : influences_column_s) {
                    String source_column_name = influencesColumn.get("source_column_name").toString();
                    if (!set.contains(source_column_name)) {
                        set.add(source_column_name);
                        Map<String, Object> colMap = new HashMap<>();
                        colMap.put("name", source_column_name);
                        colMap.put("children", nextColData(db, table_name, source_column_name));
                        influencesResult.add(colMap);
                    }
                }
            }
        } else {
            throw new BusinessException("搜索类型不匹配! search_type=" + search_type);
        }
        Map<String, Object> influencesDataInfoMap = new HashMap<>();
        influencesDataInfoMap.put("name", table_name);
        influencesDataInfoMap.put("children", influencesResult);
        return influencesDataInfoMap;
    }

    private static List<Map<String, Object>> nextColData(DatabaseWrapper db, String tableName, String sourceColumnName) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT dds.own_source_table_name AS source_table_name,djti.jobtab_en_name AS target_table_name,dmi.tar_field_name AS target_column_name,dmi.src_fields_name AS source_column_name ");
        sb.append("FROM dm_datatable_source dds JOIN dm_job_table_info djti ON dds.jobtab_id = djti.jobtab_id JOIN dm_map_info dmi ON djti.jobtab_id = dmi.jobtab_id ");
        sb.append("WHERE LOWER ( dds.own_source_table_name ) = LOWER('" + tableName + "') AND LOWER ( dmi.src_fields_name ) = LOWER('" + sourceColumnName + "') ");
        sb.append(" ORDER BY dds.own_source_table_name,dmi.src_fields_name");
        List<Map<String, Object>> childrenCols = SqlOperator.queryList(db, sb.toString());
        if (!childrenCols.isEmpty()) {
            Map<String, List<Map<String, Object>>> dataMap = new HashMap<>();
            for (Map<String, Object> childrenCol : childrenCols) {
                String target_table_name = childrenCol.get("target_table_name").toString();
                String target_column_name = childrenCol.get("target_column_name").toString();
                if (!dataMap.containsKey(target_table_name)) {
                    List<Map<String, Object>> colList = new ArrayList<>();
                    Map<String, Object> colMap = new HashMap<>();
                    colMap.put("name", target_column_name);
                    colList.add(colMap);
                    dataMap.put(target_table_name, colList);
                } else {
                    List<Map<String, Object>> colList = dataMap.get(target_table_name);
                    Boolean flag = false;
                    for (Map<String, Object> map : colList) {
                        if (map.get("name").toString().equals(target_column_name)) {
                            flag = true;
                        }
                    }
                    if (!flag) {
                        Map<String, Object> colMap = new HashMap<>();
                        colMap.put("name", target_column_name);
                        dataMap.get(target_table_name).add(colMap);
                    }
                }
            }
            Set<Map.Entry<String, List<Map<String, Object>>>> entries = dataMap.entrySet();
            for (Map.Entry<String, List<Map<String, Object>>> entry : entries) {
                Map<String, Object> map = new HashMap<>();
                map.put("name", entry.getKey());
                map.put("children", entry.getValue());
                resultList.add(map);
            }
            return resultList;
        }
        return null;
    }

    private static List<Map<String, Object>> nextData(DatabaseWrapper db, String tableName) {
        List<Map<String, Object>> resultList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT dds.own_source_table_name AS source_table_name,djti.jobtab_en_name AS target_table_name ");
        sb.append("FROM dm_datatable_source dds JOIN dm_job_table_info djti ON dds.jobtab_id = djti.jobtab_id ");
        sb.append(" WHERE LOWER(dds.own_source_table_name) = LOWER('" + tableName + "')" + "order by dds.own_source_table_name,djti.jobtab_en_name");
        List<Map<String, Object>> influences_children_table_s = SqlOperator.queryList(db, sb.toString());
        if (!influences_children_table_s.isEmpty()) {
            Set<String> set = new HashSet<>();
            influences_children_table_s.forEach(childrenTab -> {
                String target_table_name = childrenTab.get("target_table_name").toString();
                if (!set.contains(target_table_name)) {
                    set.add(target_table_name);
                    Map<String, Object> map = new HashMap<>();
                    map.put("name", target_table_name);
                    map.put("children", nextData(db, target_table_name));
                    resultList.add(map);
                }
            });
            return resultList;
        }
        return null;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "db", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Param(name = "search_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, Object> bloodlineDateInfo(DatabaseWrapper db, String table_name, String search_type) {
        Validator.notBlank(search_type, "搜索关系为空!");
        List<Map<String, Object>> children_s = new ArrayList<>();
        IsFlag is_st = IsFlag.ofEnumByCode(search_type);
        if (is_st == IsFlag.Fou) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT dmt.module_table_en_name AS TABLE_NAME,dds.own_source_table_name AS source_table_name ");
            sb.append("FROM dm_module_table dmt JOIN dm_job_table_info djti ON dmt.module_table_id = djti.module_table_id JOIN dm_datatable_source dds ON djti.jobtab_id = dds.jobtab_id");
            sb.append(" WHERE LOWER(dmt.module_table_en_name) = LOWER('" + table_name + "')" + "order by dmt.module_table_en_name,dds.own_source_table_name");
            List<Map<String, Object>> blood_table = SqlOperator.queryList(db, sb.toString());
            if (!blood_table.isEmpty()) {
                Set<String> set = new HashSet<>();
                blood_table.forEach(bloodline_data -> {
                    String tableName = bloodline_data.get("source_table_name").toString();
                    if (!set.contains(tableName)) {
                        Map<String, Object> map = new HashMap<>();
                        set.add(tableName);
                        map.put("name", tableName);
                        children_s.add(map);
                    }
                });
            }
        } else if (is_st == IsFlag.Shi) {
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT dmt.module_table_en_name AS TABLE_NAME,tfi.field_en_name AS target_column_name ");
            sb.append("FROM dm_module_table dmt JOIN dm_module_table_field_info tfi ON dmt.module_table_id = tfi.module_table_id ");
            sb.append("WHERE LOWER(dmt.module_table_en_name) = LOWER('" + table_name + "')" + " ORDER BY dmt.module_table_en_name");
            List<Map<String, Object>> blood_col_s = SqlOperator.queryList(db, sb.toString());
            if (!blood_col_s.isEmpty()) {
                Set<String> set = new HashSet<>();
                blood_col_s.forEach(blood_col -> {
                    String module_col = blood_col.get("target_column_name").toString();
                    if (!set.contains(module_col)) {
                        set.add(module_col);
                        Map<String, Object> col_map = new HashMap<String, Object>();
                        col_map.put("name", module_col);
                        col_map.put("children", nextSourceData(db, table_name, module_col));
                        children_s.add(col_map);
                    }
                });
            }
        } else {
            throw new BusinessException("搜索类型不匹配! search_type=" + search_type);
        }
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("name", table_name);
        resultMap.put("children", children_s);
        return resultMap;
    }

    private static List<Map<String, Object>> nextSourceData(DatabaseWrapper db, String tableName, String moduleCol) {
        List<Map<String, Object>> childrenResultList = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT dmt.module_table_en_name AS TABLE_NAME,dds.own_source_table_name AS source_table_name,dmi.tar_field_name AS target_column_name,dmi.src_fields_name AS source_column_name ");
        sb.append("FROM dm_module_table dmt JOIN dm_job_table_info djti ON djti.module_table_id = dmt.module_table_id JOIN dm_datatable_source dds ON djti.jobtab_id = dds.jobtab_id JOIN dm_map_info dmi ON dds.own_source_table_id = dmi.own_source_table_id ");
        sb.append("WHERE LOWER(dmt.module_table_en_name)=LOWER('" + tableName + "') ");
        sb.append("AND LOWER(dmi.tar_field_name) = LOWER('" + moduleCol + "')  ORDER BY TABLE_NAME");
        List<Map<String, Object>> children_source_data_s = SqlOperator.queryList(db, sb.toString());
        if (!children_source_data_s.isEmpty()) {
            Map<String, List<Map<String, Object>>> dataMap = new HashMap<>();
            for (Map<String, Object> childrenSourceData : children_source_data_s) {
                String source_table_name = childrenSourceData.get("source_table_name").toString();
                String source_column_name = childrenSourceData.get("source_column_name").toString();
                if (!dataMap.containsKey(source_table_name)) {
                    List<Map<String, Object>> children_source_col_s = new ArrayList<>();
                    Map<String, Object> col_map = new HashMap<>();
                    col_map.put("name", source_column_name);
                    children_source_col_s.add(col_map);
                    dataMap.put(source_table_name, children_source_col_s);
                } else {
                    List<Map<String, Object>> childrenList = dataMap.get(source_table_name);
                    Boolean flag = false;
                    for (Map<String, Object> map : childrenList) {
                        if (map.get("name").toString().equals(source_column_name)) {
                            flag = true;
                        }
                    }
                    if (!flag) {
                        Map<String, Object> colMap = new HashMap<>();
                        colMap.put("name", source_column_name);
                        dataMap.get(source_table_name).add(colMap);
                    }
                }
            }
            Set<Map.Entry<String, List<Map<String, Object>>>> entries = dataMap.entrySet();
            for (Map.Entry<String, List<Map<String, Object>>> entry : entries) {
                Map<String, Object> map = new HashMap<>();
                map.put("name", entry.getKey());
                map.put("children", entry.getValue());
                childrenResultList.add(map);
            }
            return childrenResultList;
        }
        return null;
    }
}
