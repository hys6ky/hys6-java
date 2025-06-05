package hyren.serv6.agent.job.biz.core.databaseadditinfo.impl;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.databaseadditinfo.DatabaseAdditInfoOperateInterface;
import hyren.serv6.agent.job.biz.utils.SQLUtil;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
@DocClass(desc = "", author = "zxz", createdate = "2020/5/12 18:02")
public class PostgresqlAdditInfoOperateImpl implements DatabaseAdditInfoOperateInterface {

    private static final String PK = "_hyren_pk";

    private static final String INDEX = "_hyren_ix";

    @Override
    public void addNormalIndex(String tableName, List<String> columns, DatabaseWrapper db) {
        if (columns != null && columns.size() > 0) {
            for (String column : columns) {
                String indexName = column + INDEX;
                if (!SQLUtil.indexIfExistForPostgresql(indexName, db)) {
                    long startTime = System.currentTimeMillis();
                    String sb = "create index " + indexName + " on " + tableName + Constant.LXKH + column + Constant.RXKH;
                    db.execute(sb);
                    log.info("表" + tableName + "创建普通索引耗时：" + ((System.currentTimeMillis() - startTime) / 1000) + "秒");
                } else {
                    log.info("表" + tableName + "索引已经存在，无需创建");
                }
            }
        }
    }

    @Override
    public void addPkConstraint(String tableName, List<String> columns, DatabaseWrapper db) {
        if (columns != null && columns.size() > 0) {
            String pkObjectName = tableName + PK;
            if (!SQLUtil.pkIfExistForPostgresql(pkObjectName, db)) {
                long startTime = System.currentTimeMillis();
                StringBuilder sb = new StringBuilder();
                sb.append("alter table ").append(tableName);
                sb.append(" add constraint ").append(pkObjectName).append(" primary key ").append(Constant.LXKH);
                for (String column : columns) {
                    sb.append(column).append(",");
                }
                sb.delete(sb.length() - 1, sb.length());
                sb.append(Constant.RXKH);
                db.execute(sb.toString());
                log.info("表" + tableName + "添加主键约束耗时：" + ((System.currentTimeMillis() - startTime) / 1000) + "秒");
            }
        }
    }

    @Override
    public void dropIndex(String tableName, DatabaseWrapper db) {
        String normalIndexName = tableName + INDEX;
        if (SQLUtil.indexIfExistForPostgresql(normalIndexName, db)) {
            String sql = "drop index " + normalIndexName;
            db.execute(sql);
        }
    }

    @Override
    public void dropPkConstraint(String tableName, DatabaseWrapper db) {
        String pkObjectName = tableName + PK;
        if (SQLUtil.pkIfExistForPostgresql(pkObjectName, db)) {
            String sql = "alter table " + tableName + " drop constraint " + pkObjectName;
            db.execute(sql);
        }
    }
}
