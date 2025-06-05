package hyren.serv6.commons.utils.datastorage.dcl;

import fd.ng.core.annotation.DocClass;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.entity.DefaultLengthMapping;
import hyren.serv6.commons.utils.datastorage.yamldata.YamlDataFormat;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "dhw", createdate = "2021-12-20 15:45")
public class LengthMapping implements YamlDataFormat {

    public static final String PREFIX = "defaultlengthmapping";

    public static final String NAME = "NAME";

    @Override
    public Map<String, List<Map<String, Object>>> yamlDataFormat(String dbBatch_row) {
        Map<String, Map<String, Object>> contrastMap = getDefaultLengthMap();
        List<Map<String, Object>> typeLengthContrastAll = new ArrayList<>();
        contrastMap.forEach((database_name, typeContrast) -> {
            typeLengthContrastAll.add(typeContrast);
        });
        Map<String, List<Map<String, Object>>> map = new LinkedHashMap<>();
        map.put(LengthMapping.PREFIX, typeLengthContrastAll);
        return map;
    }

    public static Map<String, Map<String, Object>> getDefaultLengthMap() {
        Map<String, Map<String, Object>> defaultLengthMap = new LinkedHashMap<>();
        getDefaultLengthMapping().forEach(item -> {
            String database_name = ((String) item.get("database_name")).toUpperCase();
            String column_type = ((String) item.get("column_type")).toUpperCase();
            Object column_length = item.get("column_length");
            if (defaultLengthMap.containsKey(database_name)) {
                defaultLengthMap.get(database_name).put(column_type, column_length);
            } else {
                Map<String, Object> map = new LinkedHashMap<>();
                map.put(NAME, database_name);
                map.put(column_type, column_length);
                defaultLengthMap.put(database_name, map);
            }
        });
        return defaultLengthMap;
    }

    private static List<Map<String, Object>> getDefaultLengthMapping() {
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            return SqlOperator.queryList(db, "select * from " + DefaultLengthMapping.TableName + " order by database_name");
        }
    }
}
