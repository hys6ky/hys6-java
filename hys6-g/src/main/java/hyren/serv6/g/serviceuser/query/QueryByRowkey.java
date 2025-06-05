package hyren.serv6.g.serviceuser.query;

import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.daos.base.utils.ActionResult;
import hyren.serv6.commons.hadoop.i.IHbase;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.constant.CommonVariables;
import hyren.serv6.commons.utils.constant.Constant;
import hyren.serv6.g.enumerate.StateType;
import hyren.serv6.g.init.InterfaceManager;
import hyren.serv6.g.serviceuser.common.InterfaceCommon;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class QueryByRowkey {

    private static final Logger logger = LogManager.getLogger();

    public static ActionResult query(String en_table, String rowkey, String en_column, String versions, String dsl_name, String platform, String prncipal_name, String hadoop_user_name, long user_id, DatabaseWrapper db) {
        ActionResult actionResult;
        if (StringUtil.isBlank(en_table) || StringUtil.isBlank(rowkey)) {
            actionResult = StateType.getActionResult(StateType.ARGUMENT_ERROR);
            actionResult.setData("表名或者rowkey不能为空");
        } else {
            List<String> colWithCf = new ArrayList<>();
            if (StringUtil.isNotBlank(en_column)) {
                String table_en_column = InterfaceManager.getUserTableInfo(db, user_id, en_table).getTable_en_column();
                List<String> columns = StringUtil.split(table_en_column.toLowerCase(), Constant.METAINFOSPLIT);
                ActionResult userColumn = checkRowkeyColumnsIsExist(en_column, user_id, columns);
                if (userColumn != null)
                    return userColumn;
                List<String> colList = StringUtil.split(en_column, "|");
                for (String colsWithCf : colList) {
                    if (!colsWithCf.contains(":")) {
                        return StateType.getActionResult(StateType.ROWKEY_COLUMN_FORMAT_ERROR);
                    }
                    colWithCf = StringUtil.split(colsWithCf, ":");
                }
            }
            IHbase iHbase = ClassBase.HbaseInstance();
            try {
                List<Map<String, Object>> message = iHbase.queryByRowkey(en_table, rowkey, versions, dsl_name, platform, prncipal_name, hadoop_user_name, colWithCf);
                actionResult = StateType.getActionResult(StateType.NORMAL);
                actionResult.setData(message);
            } catch (IOException ioException) {
                actionResult = StateType.getActionResult(StateType.ARGUMENT_ERROR);
                actionResult.setData("ioException: " + en_table);
                logger.error(ioException);
            } catch (Exception e) {
                actionResult = StateType.getActionResult(StateType.EXCEPTION);
                actionResult.setData(e.getMessage());
                logger.error(e);
            }
        }
        return actionResult;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "selectColumn", desc = "", range = "")
    @Param(name = "user_id", desc = "", range = "")
    @Param(name = "columns", desc = "", range = "")
    @Return(desc = "", range = "")
    public static ActionResult checkRowkeyColumnsIsExist(String selectColumn, Long user_id, List<String> columns) {
        if (StringUtil.isNotBlank(selectColumn)) {
            if (!CommonVariables.AUTHORITY.contains(String.valueOf(user_id))) {
                List<String> userColumns = StringUtil.split(selectColumn, "|");
                if (columns != null && columns.size() != 0) {
                    for (String userColumn : userColumns) {
                        List<String> columnList = StringUtil.split(userColumn, ":");
                        if (InterfaceCommon.columnIsExist(columnList.get(1).toLowerCase(), columns)) {
                            ActionResult actionResult = StateType.getActionResult(StateType.NO_COLUMN_USE_PERMISSIONS);
                            actionResult.setData("列名" + columnList.get(1) + "没有使用权限");
                            return actionResult;
                        }
                    }
                }
            }
        }
        return null;
    }
}
