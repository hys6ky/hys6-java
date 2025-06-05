package hyren.serv6.agent.job.biz.core.increasement;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.commons.hadoop.sqlutils.HSqlExecute;
import hyren.serv6.commons.utils.agent.Increasement;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import java.io.Closeable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "zxz", createdate = "2019/12/24 09:41")
public abstract class JDBCIncreasement implements Closeable, Increasement {

    protected List<String> columns;

    protected List<String> types;

    protected String sysDate;

    protected String tableNameInHBase;

    protected String deltaTableName;

    protected String yesterdayTableName;

    protected DatabaseWrapper db;

    protected String todayTableName;

    protected List<String> tar_types;

    protected String storage_type;

    protected Long storage_time;

    protected JDBCIncreasement(TableBean tableBean, String hbase_name, String sysDate, DatabaseWrapper db, Long dsl_id) {
        this.columns = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        this.types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        this.sysDate = sysDate;
        this.tableNameInHBase = hbase_name;
        this.deltaTableName = hbase_name + "_hy";
        this.yesterdayTableName = hbase_name;
        this.todayTableName = TableNameUtil.getUnderline1TableName(hbase_name, tableBean.getStorage_type(), tableBean.getStorage_time());
        this.db = db;
        Map<Long, String> tbColTarMap = tableBean.getTbColTarMap();
        String tbColTarType = tbColTarMap.get(dsl_id);
        List<String> tar_types = new ArrayList<>();
        if (StringUtil.isNotBlank(tbColTarType)) {
            tar_types = StringUtil.split(tbColTarType, Constant.METAINFOSPLIT);
        }
        this.tar_types = tar_types;
        this.storage_type = tableBean.getStorage_type();
        this.storage_time = tableBean.getStorage_time();
    }

    protected String insertDeltaDataSql(String targetTableName, String sourceTableName) {
        StringBuilder insertDataSql = new StringBuilder(120);
        insertDataSql.append("INSERT INTO ");
        Dbtype dbType = db.getDbtype();
        if (dbType == Dbtype.KINGBASE) {
            insertDataSql.append(db.getDatabaseName()).append('.').append(targetTableName);
        } else {
            insertDataSql.append(targetTableName);
        }
        insertDataSql.append(" select ");
        columns = dbType.ofEscapedkey(columns);
        for (String col : columns) {
            if (dbType == Dbtype.KINGBASE) {
                insertDataSql.append(db.getDatabaseName()).append('.').append(sourceTableName).append(".").append(col).append(",");
            } else {
                if (!Constant.HYRENFIELD.contains(col.toUpperCase())) {
                    insertDataSql.append(sourceTableName).append(".").append(col).append(",");
                } else {
                    insertDataSql.append(sourceTableName).append(".").append(col).append(",");
                }
            }
        }
        insertDataSql.deleteCharAt(insertDataSql.length() - 1);
        insertDataSql.append(" from ");
        if (dbType == Dbtype.KINGBASE) {
            insertDataSql.append(db.getDatabaseName()).append('.').append(sourceTableName);
        } else {
            insertDataSql.append(sourceTableName);
        }
        return insertDataSql.toString();
    }

    public static void dropTableIfExists(String tableName, DatabaseWrapper db, List<String> sqlList) {
        if (db.isExistTable(tableName)) {
            if (db.getDbtype() == Dbtype.KINGBASE) {
                sqlList.add("DROP TABLE " + db.getDatabaseName() + '.' + tableName);
            } else {
                sqlList.add("DROP TABLE " + tableName);
            }
        }
    }

    public static void dropTableIfExists(String tableName, DatabaseWrapper db) {
        if (db.isExistTable(tableName)) {
            HSqlExecute.executeSql("DROP TABLE " + tableName, db);
        }
    }
}
