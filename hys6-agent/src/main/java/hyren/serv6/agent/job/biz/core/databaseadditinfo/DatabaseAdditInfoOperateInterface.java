package hyren.serv6.agent.job.biz.core.databaseadditinfo;

import fd.ng.db.jdbc.DatabaseWrapper;
import java.util.List;

public interface DatabaseAdditInfoOperateInterface {

    void addNormalIndex(String tableName, List<String> columns, DatabaseWrapper db);

    void addPkConstraint(String tableName, List<String> columns, DatabaseWrapper db);

    void dropIndex(String tableName, DatabaseWrapper db);

    void dropPkConstraint(String tableName, DatabaseWrapper db);
}
