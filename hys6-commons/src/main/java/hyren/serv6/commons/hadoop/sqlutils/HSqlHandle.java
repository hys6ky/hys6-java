package hyren.serv6.commons.hadoop.sqlutils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.serv6.commons.hadoop.util.ClassBase;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.util.ArrayList;
import java.util.List;

@DocClass(desc = "", author = "zxz", createdate = "2019/10/25 9:57")
@Slf4j
public class HSqlHandle {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Param(name = "hiveColumns", desc = "", range = "")
    @Param(name = "hiveTypes", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<String> hiveMapHBase(String tableName, String hiveColumns, String hiveTypes) {
        StringBuilder sql = new StringBuilder(1024);
        sql.append("drop table if exists ").append(tableName);
        List<String> sqlList = new ArrayList<>();
        sqlList.add(sql.toString());
        sql.delete(0, sql.length());
        sql.append("create external table if not exists ").append(tableName);
        sql.append(" ( ").append(Constant.HBASE_ROW_KEY).append(" string , ");
        String[] csvStructVal = hiveColumns.split(",");
        String[] csvStructType = hiveTypes.split(",");
        for (int i = 0; i < csvStructVal.length; i++) {
            sql.append("`").append(csvStructVal[i]).append("` ").append(csvStructType[i]).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append(") row format delimited fields terminated by '\\t' ");
        sql.append("STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' ");
        sql.append("WITH SERDEPROPERTIES (\"hbase.columns.mapping\" = \":key , ");
        for (String col : csvStructVal) {
            sql.append(ClassBase.hadoopInstance().byteToString(Constant.HBASE_COLUMN_FAMILY)).append(":").append(col).append(",");
        }
        sql.deleteCharAt(sql.length() - 1);
        sql.append("\") TBLPROPERTIES (\"hbase.table.name\" = \"").append(tableName).append("\")");
        sqlList.add(sql.toString());
        log.info("拼接hbase映射hive表的sql为=====" + sql.toString());
        return sqlList;
    }
}
