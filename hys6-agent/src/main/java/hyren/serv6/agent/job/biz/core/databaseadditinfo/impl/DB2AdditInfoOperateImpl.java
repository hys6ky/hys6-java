package hyren.serv6.agent.job.biz.core.databaseadditinfo.impl;

import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.agent.job.biz.core.databaseadditinfo.DatabaseAdditInfoOperateInterface;
import hyren.serv6.agent.job.biz.utils.SQLUtil;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public class DB2AdditInfoOperateImpl implements DatabaseAdditInfoOperateInterface {

    private static final String PK = "_hyren_pk";

    private static final String INDEX = "_hyren_ix";

    @Override
    public void addNormalIndex(String tableName, List<String> columns, DatabaseWrapper db) {
        log.info("db2 addNormalIndex");
    }

    @Override
    public void addPkConstraint(String tableName, List<String> columns, DatabaseWrapper db) {
        if (columns != null && columns.size() > 0) {
            String pkObjectName = tableName + PK;
            if (SQLUtil.pkIfExistForDB2(tableName, db)) {
                log.info(db.getDbtype() + " 表: " + tableName + " ,已经存在主键! 删除额外的,取页面配置信息做主键");
                dropPkConstraint(tableName, db);
            }
            long startTime = System.currentTimeMillis();
            StringBuilder sb = new StringBuilder();
            sb.append("alter table ").append(tableName);
            sb.append(" add constraint ").append(pkObjectName).append(" primary key ").append(Constant.LXKH);
            for (String column : columns) {
                sb.append(column).append(",");
                db.execute("ALTER TABLE " + tableName + " ALTER " + column + " SET NOT NULL");
                db.execute("call sysproc.admin_cmd('reorg table " + tableName + "')");
            }
            sb.delete(sb.length() - 1, sb.length());
            sb.append(Constant.RXKH);
            db.execute(sb.toString());
            db.execute("call sysproc.admin_cmd('reorg table " + tableName + "')");
            log.info(db.getDbtype() + " 表: " + tableName + " ,添加主键约束耗时: " + ((System.currentTimeMillis() - startTime) / 1000) + "秒");
        }
    }

    @Override
    public void dropIndex(String tableName, DatabaseWrapper db) {
        log.info("db2 dropIndex");
    }

    @Override
    public void dropPkConstraint(String tableName, DatabaseWrapper db) {
        if (SQLUtil.pkIfExistForDB2(tableName, db)) {
            db.execute("alter table " + tableName + " DROP PRIMARY KEY");
            db.execute("call sysproc.admin_cmd('reorg table " + tableName + "')");
        }
    }
}
