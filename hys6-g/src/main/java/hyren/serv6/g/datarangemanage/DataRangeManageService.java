package hyren.serv6.g.datarangemanage;

import com.fasterxml.jackson.core.type.TypeReference;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.JsonUtil;
import fd.ng.core.utils.StringUtil;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.datatree.background.TreeNodeInfo;
import hyren.serv6.base.datatree.background.bean.TreeConf;
import hyren.serv6.base.datatree.tree.Node;
import hyren.serv6.base.datatree.tree.NodeDataConvertedTreeList;
import hyren.serv6.base.datatree.tree.TreePageSource;
import hyren.serv6.base.entity.SysregParameterInfo;
import hyren.serv6.base.entity.TableColumn;
import hyren.serv6.base.entity.TableUseInfo;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.base.key.PrimayKeyGener;
import hyren.serv6.base.user.UserUtil;
import hyren.serv6.commons.utils.datatable.DataTableUtil;
import hyren.serv6.g.bean.TableDataInfo;
import hyren.serv6.g.init.InterfaceManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.stereotype.Service;
import java.util.List;
import java.util.Map;

@Service
public class DataRangeManageService {

    private static final Logger logger = LogManager.getLogger();

    @Method(desc = "", logicStep = "")
    @Return(desc = "", range = "")
    public List<Node> searchDataUsageRangeInfoToTreeData() {
        TreeConf treeConf = new TreeConf();
        treeConf.setShowFileCollection(Boolean.FALSE);
        List<Map<String, Object>> dataList = TreeNodeInfo.getTreeNodeInfo(TreePageSource.INTERFACE, UserUtil.getUser(), treeConf);
        return NodeDataConvertedTreeList.dataConversionTreeInfo(dataList);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "", nullable = true)
    @Param(name = "data_layer", desc = "", range = "")
    @Return(desc = "", range = "")
    public Map<String, Object> searchFieldById(String file_id, String data_layer) {
        return DataTableUtil.getTableInfoAndColumnInfo(data_layer, file_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableDataInfos", desc = "", range = "", isBean = true)
    @Param(name = "table_note", desc = "", range = "", nullable = true)
    @Param(name = "data_layer", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    public void saveTableData(TableDataInfo[] tableDataInfos, String table_note, String data_layer, Long[] user_id) {
        for (TableDataInfo tableDataInfo : tableDataInfos) {
            Map<String, Object> tableInfoAndColumnInfo = DataTableUtil.getTableInfoAndColumnInfo(data_layer, tableDataInfo.getFile_id());
            for (long userId : user_id) {
                TableUseInfo table_use_info = new TableUseInfo();
                String hyren_name = tableInfoAndColumnInfo.get("hyren_name").toString();
                String original_name;
                if (tableInfoAndColumnInfo.get("original_name") == null) {
                    original_name = tableInfoAndColumnInfo.get("table_name").toString();
                } else {
                    original_name = tableInfoAndColumnInfo.get("original_name").toString();
                }
                boolean flag = getUserTableInfo(userId, hyren_name);
                String useId = String.valueOf(PrimayKeyGener.getNextId());
                if (flag) {
                    deleteInterfaceTableInfo(userId, hyren_name);
                }
                addTableUseInfo(table_note, data_layer, userId, useId, table_use_info, hyren_name, original_name);
                String[] table_ch_column = tableDataInfo.getTable_ch_column();
                String[] table_en_column = tableDataInfo.getTable_en_column();
                if (table_ch_column.length == 0 && table_en_column.length == 0) {
                    List<TableColumn> tableColumnList = JsonUtil.toObject(JsonUtil.toJson(tableInfoAndColumnInfo.get("column_info_list")), new TypeReference<List<TableColumn>>() {
                    });
                    table_ch_column = new String[tableColumnList.size()];
                    table_en_column = new String[tableColumnList.size()];
                    for (int i = 0; i < tableColumnList.size(); i++) {
                        table_ch_column[i] = tableColumnList.get(i).getColumn_ch_name();
                        table_en_column[i] = tableColumnList.get(i).getColumn_name();
                    }
                }
                addSysRegParameterInfo(useId, userId, table_ch_column, table_en_column, tableInfoAndColumnInfo.get("column_info_list"));
            }
            InterfaceManager.initTable(Dbo.db());
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "sysreg_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public boolean getUserTableInfo(long user_id, String sysreg_name) {
        if (Dbo.queryNumber("select count(1) from " + TableUseInfo.TableName + " where user_id = ? and sysreg_name = ?", user_id, sysreg_name).orElseThrow(() -> new BusinessException("sql查询错误")) > 0) {
            logger.info("此表已登记");
            return true;
        } else {
            logger.info("此表未登记");
            return false;
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "sysreg_name", desc = "", range = "")
    private void deleteInterfaceTableInfo(long user_id, String sysreg_name) {
        List<Long> useIdList = Dbo.queryOneColumnList("select use_id from " + TableUseInfo.TableName + " where lower(sysreg_name)=lower(?) and user_id=?", sysreg_name, user_id);
        for (Long use_id : useIdList) {
            Dbo.execute("delete from " + SysregParameterInfo.TableName + " where use_id =? ", use_id);
        }
        Dbo.execute("delete from " + TableUseInfo.TableName + " where lower(sysreg_name) = lower(?)" + " and user_id = ?", sysreg_name, user_id);
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_note", desc = "", range = "")
    @Param(name = "data_layer", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "useId", desc = "", range = "")
    @Param(name = "table_use_info", desc = "", range = "", isBean = true)
    @Param(name = "hyren_name", desc = "", range = "")
    @Param(name = "original_name", desc = "", range = "")
    private void addTableUseInfo(String table_note, String data_layer, long userId, String useId, TableUseInfo table_use_info, String hyren_name, String original_name) {
        table_use_info.setSysreg_name(hyren_name);
        table_use_info.setUser_id(userId);
        table_use_info.setUse_id(Long.parseLong(useId));
        table_use_info.setTable_blsystem(data_layer);
        if (StringUtil.isBlank(original_name)) {
            table_use_info.setOriginal_name(hyren_name);
        } else {
            table_use_info.setOriginal_name(original_name);
        }
        if (StringUtil.isBlank(table_note)) {
            table_use_info.setTable_note("");
        } else {
            table_use_info.setTable_note(table_note);
        }
        table_use_info.add(Dbo.db());
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "tableDataInfo", desc = "", range = "")
    @Param(name = "useId", desc = "", range = "")
    @Param(name = "userId", desc = "", range = "")
    @Param(name = "table_ch_column", desc = "", range = "")
    @Param(name = "table_en_column", desc = "", range = "")
    @Param(name = "column_info_list", desc = "", range = "")
    private void addSysRegParameterInfo(String useId, long userId, String[] table_ch_column, String[] table_en_column, Object column_info_list) {
        SysregParameterInfo sysreg_parameter_info = new SysregParameterInfo();
        sysreg_parameter_info.setUse_id(Long.parseLong(useId));
        sysreg_parameter_info.setIs_flag(IsFlag.Fou.getCode());
        sysreg_parameter_info.setUser_id(userId);
        System.out.println(JsonUtil.toJson(column_info_list));
        List<Map<String, String>> columnInfoList = JsonUtil.toObject(JsonUtil.toJson(column_info_list), new TypeReference<List<Map<String, String>>>() {
        });
        for (int i = 0; i < table_en_column.length; i++) {
            sysreg_parameter_info.setParameter_id(PrimayKeyGener.getNextId());
            sysreg_parameter_info.setTable_ch_column(table_ch_column[i]);
            sysreg_parameter_info.setTable_en_column(table_en_column[i]);
            for (Map<String, String> map : columnInfoList) {
                if (map.get("column_name").equals(table_en_column[i])) {
                    sysreg_parameter_info.setRemark(JsonUtil.toJson(map));
                }
            }
            sysreg_parameter_info.add(Dbo.db());
        }
    }
}
