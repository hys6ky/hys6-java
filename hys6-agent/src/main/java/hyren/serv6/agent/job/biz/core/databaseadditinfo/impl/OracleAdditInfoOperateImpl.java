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
public class OracleAdditInfoOperateImpl implements DatabaseAdditInfoOperateInterface {

    private static final String PK = "_pk";

    private static final String INDEX = "_ix";

    @Override
    public void addNormalIndex(String tableName, List<String> columns, DatabaseWrapper db) {
        if (columns != null && columns.size() > 0) {
            for (String column : columns) {
                String indexName = getIndexName(column, tableName);
                if (!SQLUtil.objectIfExistForOracle(indexName, tableName, db)) {
                    long startTime = System.currentTimeMillis();
                    StringBuilder sb = new StringBuilder();
                    sb.append("create index ").append(indexName);
                    sb.append(" on ").append(tableName).append(Constant.LXKH);
                    sb.append(column).append(Constant.RXKH).append(" online");
                    db.execute(sb.toString());
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
            String pkObjectName = getpkObjectName(tableName);
            if (!SQLUtil.objectIfExistForOracle(pkObjectName, tableName, db)) {
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
        if (SQLUtil.objectIfExistForOracle(normalIndexName, tableName, db)) {
            String sql = "drop index " + normalIndexName;
            db.execute(sql);
        }
    }

    @Override
    public void dropPkConstraint(String tableName, DatabaseWrapper db) {
        String pkObjectName = getpkObjectName(tableName);
        if (SQLUtil.objectIfExistForOracle(pkObjectName, tableName, db)) {
            String sql = "alter table " + tableName + " drop primary key";
            db.execute(sql);
        }
    }

    private String getIndexName(String column, String tableName) {
        String indexName = column + INDEX + '_' + tableName;
        indexName = indexName.length() > 30 ? indexName.substring(indexName.length() - 30) : indexName;
        indexName = indexName.startsWith(Constant.SPLITTER) ? indexName.replaceFirst(Constant.SPLITTER, Constant.SPACE) : indexName;
        return indexName;
    }

    private String getpkObjectName(String tableName) {
        return tableName + PK;
    }
}
