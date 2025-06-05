package hyren.serv6.hadoop.increasement;

import fd.ng.core.utils.FileNameUtils;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.conf.Dbtype;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.hadoop.commons.hadoop_helper.HBaseOperator;
import hyren.serv6.commons.hadoop.sqlutils.HSqlExecute;
import hyren.serv6.commons.key.HashChoreWoker;
import hyren.serv6.commons.utils.agent.Increasement;
import hyren.serv6.commons.utils.agent.TableNameUtil;
import hyren.serv6.commons.utils.agent.bean.TableBean;
import hyren.serv6.commons.utils.constant.Constant;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class HBaseIncreasement implements Closeable, Increasement {

    protected DatabaseWrapper db;

    protected List<String> columns;

    protected List<String> types;

    protected String sysDate;

    protected String tableNameInHBase;

    protected String deltaTableName;

    protected String yesterdayTableName;

    protected HBaseOperator hBaseOperator;

    protected String todayTableName;

    protected List<String> tar_types;

    protected Long storage_time;

    protected HBaseIncreasement(TableBean tableBean, String hbase_name, String sysDate, String dsl_name, String hadoop_user_name, String platform, String prncipal_name, Long dsl_id, DatabaseWrapper db) {
        this.columns = StringUtil.split(tableBean.getColumnMetaInfo(), Constant.METAINFOSPLIT);
        this.types = StringUtil.split(tableBean.getColTypeMetaInfo(), Constant.METAINFOSPLIT);
        this.sysDate = sysDate;
        this.tableNameInHBase = hbase_name;
        this.deltaTableName = hbase_name + "_hy";
        this.yesterdayTableName = hbase_name;
        this.storage_time = tableBean.getStorage_time();
        Map<Long, String> tbColTarMap = tableBean.getTbColTarMap();
        String tbColTarType = tbColTarMap.get(dsl_id);
        List<String> tar_types = new ArrayList<>();
        if (StringUtil.isNotBlank(tbColTarType)) {
            tar_types = StringUtil.split(tbColTarType, Constant.METAINFOSPLIT);
        }
        this.tar_types = tar_types;
        this.todayTableName = TableNameUtil.getUnderline1TableName(hbase_name, tableBean.getStorage_type(), tableBean.getStorage_time());
        this.db = db;
        this.hBaseOperator = new HBaseOperator(FileNameUtils.normalize(Constant.STORECONFIGPATH + dsl_name + File.separator, true), platform, prncipal_name, hadoop_user_name);
    }

    @Override
    public void close() {
        dropAllTmpTable();
        try {
            if (db != null) {
                db.close();
            }
            if (hBaseOperator != null) {
                hBaseOperator.close();
            }
        } catch (IOException e) {
            throw new AppSystemException(String.format("关闭连接失败：%s", e));
        }
    }

    public static void createDefaultPrePartTable(HBaseOperator hBaseOperator, String table, boolean snappycompress) {
        HashChoreWoker worker = new HashChoreWoker(1000000, 10);
        byte[][] splitKeys = worker.calcSplitKeys();
        hBaseOperator.createTable(table, splitKeys, snappycompress, Bytes.toString(Constant.HBASE_COLUMN_FAMILY));
    }

    private void dropAllTmpTable() {
        List<String> deleteInfo = new ArrayList<>();
        if (db.isExistTable(deltaTableName)) {
            if (db.getDbtype() == Dbtype.KINGBASE) {
                deleteInfo.add("DROP TABLE " + db.getDatabaseName() + '.' + deltaTableName);
            } else {
                deleteInfo.add("DROP TABLE " + deltaTableName);
            }
        }
        HSqlExecute.executeSql(deleteInfo, db);
    }

    public void dropTodayTable() {
        List<String> deleteInfo = new ArrayList<>();
        if (db.isExistTable(deltaTableName)) {
            if (db.getDbtype() == Dbtype.KINGBASE) {
                deleteInfo.add("DROP TABLE " + db.getDatabaseName() + '.' + deltaTableName);
            } else {
                deleteInfo.add("DROP TABLE " + deltaTableName);
            }
        }
        HSqlExecute.executeSql(deleteInfo, db);
        hBaseOperator.dropTable(todayTableName);
    }
}
