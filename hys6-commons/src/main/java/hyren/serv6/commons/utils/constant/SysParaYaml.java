package hyren.serv6.commons.utils.constant;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.entity.SysPara;
import java.util.*;

@DocClass(desc = "", author = "Mr.Lee", createdate = "2020-01-15 11:22")
public class SysParaYaml {

    private static final String PREFIX = "param";

    public static final String CONF_FILE_NAME = "sysparam.conf";

    public Map<String, List<Map<String, Object>>> yamlDataFormat(String dbBatch_row) {
        Map<String, List<Map<String, Object>>> map = new LinkedHashMap<>();
        Map<String, Object> sysMap = new HashMap<>();
        getSysPara().forEach(item -> {
            if (item.get("para_name").equals("dbBatch_row")) {
                sysMap.put(((String) item.get("para_name")), StringUtil.isNotBlank(dbBatch_row) && !dbBatch_row.equals("0") ? dbBatch_row : String.valueOf(item.get("para_value")));
            } else {
                sysMap.put(((String) item.get("para_name")), item.get("para_value"));
            }
        });
        List<Map<String, Object>> list = new ArrayList<>();
        list.add(sysMap);
        map.put(PREFIX, list);
        return map;
    }

    private List<Map<String, Object>> getSysPara() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "SELECT * FROM " + SysPara.TableName);
        }
    }
}
