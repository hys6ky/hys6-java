package hyren.serv6.base.datatree.background.query;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.entity.DataStoreLayer;
import hyren.serv6.base.entity.DqIndex3record;
import hyren.serv6.base.exception.BusinessException;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/1/7 0007 上午 11:17")
public class DQCDataQuery {

    @Method(desc = "", logicStep = "")
    @Param(name = "tableName", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<DataStoreLayer> getDQCDataInfos() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, DataStoreLayer.class, "SELECT DISTINCT dsl.* FROM " + DqIndex3record.TableName + " di3 JOIN " + DataStoreLayer.TableName + " dsl ON dsl.dsl_id = di3.dsl_id");
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "dsl_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, Object>> getDQCTableInfos(long dsl_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT dsl.*,di3.* FROM " + DqIndex3record.TableName + " di3 LEFT JOIN " + DataStoreLayer.TableName + " dsl ON dsl.dsl_id = di3.dsl_id where di3.dsl_id=?", dsl_id);
        }
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "file_id", desc = "", range = "")
    @Return(desc = "", range = "")
    public static DqIndex3record getDQCTableInfo(String file_id) {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            DqIndex3record dq_index3record = new DqIndex3record();
            dq_index3record.setRecord_id(Long.parseLong(file_id));
            SqlOperator.Assembler asmSql = SqlOperator.Assembler.newInstance();
            asmSql.clean();
            asmSql.addSql("SELECT * FROM " + DqIndex3record.TableName + " WHERE record_id = ?");
            asmSql.addParam(dq_index3record.getRecord_id());
            return SqlOperator.queryOneObject(db, DqIndex3record.class, asmSql.sql(), asmSql.params()).orElseThrow(() -> (new BusinessException("获取DQC层表信息的sql失败")));
        }
    }
}
