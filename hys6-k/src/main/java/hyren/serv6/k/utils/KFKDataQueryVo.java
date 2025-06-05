package hyren.serv6.k.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.datatree.background.query.KFKDataQuery;
import hyren.serv6.base.exception.BusinessException;
import hyren.serv6.commons.collection.ConnectionTool;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "yec", createdate = "2021/6/3")
public class KFKDataQueryVo extends KFKDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Param(name = "table_name", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<String> getKFKcolums(String dsl_id, String table_name) {
        try (DatabaseWrapper dbWrapper = new DatabaseWrapper()) {
            try (DatabaseWrapper db = ConnectionTool.getDBWrapper(dbWrapper, Long.parseLong(dsl_id))) {
                List<String> columnEnNameList = new ArrayList<>();
                List<String> primaryKeysList = new ArrayList<>();
                DatabaseMetaData data = db.getConnection().getMetaData();
                ResultSet columnsList = data.getColumns(null, "%", table_name, "%");
                ResultSet primaryKeys = data.getPrimaryKeys("", "", table_name);
                while (columnsList.next()) {
                    columnEnNameList.add(columnsList.getString("COLUMN_NAME"));
                }
                while (primaryKeys.next()) {
                    primaryKeysList.add(primaryKeys.getString("COLUMN_NAME"));
                }
                if (!primaryKeysList.isEmpty() && columnEnNameList.containsAll(primaryKeysList)) {
                    columnEnNameList.remove(primaryKeysList.get(0));
                    columnEnNameList.add(primaryKeysList.get(0) + "`1");
                }
                return columnEnNameList;
            } catch (Exception e) {
                throw new BusinessException(e.getMessage());
            }
        }
    }
}
