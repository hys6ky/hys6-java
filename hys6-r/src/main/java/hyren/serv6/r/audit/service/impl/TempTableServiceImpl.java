package hyren.serv6.r.audit.service.impl;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.entity.DfProInfo;
import hyren.serv6.base.entity.DfTableApply;
import hyren.serv6.base.entity.DfTableColumn;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import hyren.serv6.r.audit.service.TempTableService;
import hyren.serv6.r.record.service.impl.DfTableApplyServiceImpl;
import hyren.serv6.r.util.TempTableConf;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Service
@Slf4j
public class TempTableServiceImpl implements TempTableService {

    @Override
    public List<Map<String, Object>> queryData(long applyTableId) {
        String applySelect = "SELECT apply_tab_id, table_id, df_pid, dep_id, create_user_id, create_date, create_time, update_date, update_time, dta_remarks, dsl_table_name_id, is_sync, is_rec FROM " + DfTableApply.TableName + " where apply_tab_id = ? ";
        DfTableApply tableApply = SqlOperator.queryOneObject(Dbo.db(), DfTableApply.class, applySelect, applyTableId).get();
        if (tableApply == null) {
            throw new BusinessException("未找到申请表信息");
        }
        String proSelect = "SELECT df_pid, pro_name, df_type, user_id, submit_user, submit_date, submit_time, submit_state, dsl_id, df_remarks FROM " + DfProInfo.TableName + " where df_pid = ? ";
        DfProInfo proInfo = SqlOperator.queryOneObject(Dbo.db(), DfProInfo.class, proSelect, tableApply.getDf_pid()).get();
        if (proInfo == null) {
            throw new BusinessException("未找到项目信息");
        }
        String columnSelect = "SELECT apply_col_id, apply_tab_id, col_ch_name, col_name, col_type, col_remarks, is_primarykey FROM " + DfTableColumn.TableName + " WHERE apply_tab_id = ? ";
        List<DfTableColumn> columnList = SqlOperator.queryList(Dbo.db(), DfTableColumn.class, columnSelect, tableApply.getApply_tab_id());
        if (CollectionUtils.isEmpty(columnList)) {
            throw new BusinessException("未找到字段信息");
        }
        try (DatabaseWrapper dw = ConnectionTool.getDBWrapper(Dbo.db(), proInfo.getDsl_id())) {
            String columnString = columnList.stream().map(t -> t.getCol_name()).filter(t -> (!t.equals(TempTableConf.OPERATION_COLUMN_NAME.toLowerCase()))).collect(Collectors.joining(","));
            String tempSelect = "select " + columnString + " from " + tableApply.getDsl_table_name_id();
            List<Map<String, Object>> queryList = SqlOperator.queryList(dw, tempSelect, new Object[0]);
            DfTableApplyServiceImpl dfTableApplyService = new DfTableApplyServiceImpl();
            List<Map<String, String>> maps = JsonUtil.toObject(JsonUtil.toJson(queryList), new TypeReference<List<Map<String, String>>>() {
            });
            List<List<String>> lists = dfTableApplyService.objToArray(maps);
            List<String> columns = lists.get(0);
            List<Map<String, Object>> dbColumnList = Dbo.queryList("SELECT dtc.col_ch_name,dtc.col_name, dtc.is_primarykey FROM " + " df_table_apply dta  JOIN df_table_column dtc ON dtc.apply_tab_id = dta.apply_tab_id WHERE " + " dta.dsl_table_name_id = ?", tableApply.getDsl_table_name_id());
            List<String> newColumn = new ArrayList<>();
            for (int i = 0; i < columns.size(); i++) {
                boolean flag = false;
                for (Map<String, Object> objectMap : dbColumnList) {
                    if (columns.get(i).equals(objectMap.get("col_name").toString().toLowerCase())) {
                        String col = objectMap.get("col_ch_name") == null ? StringUtil.EMPTY : objectMap.get("col_ch_name").toString();
                        if (Integer.parseInt(objectMap.get("is_primarykey").toString()) == 1) {
                            newColumn.add(col + "-" + objectMap.get("col_name").toString() + "-1");
                        } else {
                            newColumn.add(col + "-" + objectMap.get("col_name").toString());
                        }
                        flag = true;
                        break;
                    }
                }
                if (!flag) {
                    newColumn.add(columns.get(i));
                }
            }
            List<List<String>> resultArray = new ArrayList<>();
            resultArray.add(newColumn);
            for (int i = 1; i < lists.size(); i++) {
                resultArray.add(lists.get(i));
            }
            return JsonUtil.toObject(JsonUtil.toJson(dfTableApplyService.arrayToObj(resultArray)), new TypeReference<List<Map<String, Object>>>() {
            });
        } catch (Exception e) {
            e.printStackTrace();
            throw new BusinessException("获取存储层失败");
        }
    }
}
