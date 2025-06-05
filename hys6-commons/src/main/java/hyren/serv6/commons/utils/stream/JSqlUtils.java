package hyren.serv6.commons.utils.stream;

import fd.ng.core.utils.JsonUtil;
import hyren.serv6.commons.utils.stream.common.JSqlMapData;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class JSqlUtils {

    public static String query(Map<String, Object> tabSchema, String tableName, List<List<Map<String, Object>>> dataSets, String sql) throws Exception {
        File file = createTempJson();
        List<List<String>> list = new LinkedList<List<String>>();
        for (List<Map<String, Object>> dataSet : dataSets) {
            for (Object obj : dataSet) {
                Map<String, Object> object = (Map<String, Object>) obj;
                List<String> tmp = new LinkedList<>();
                for (String key : tabSchema.keySet()) {
                    if (Objects.isNull(object.get(key))) {
                        tmp.add(" ");
                    } else {
                        tmp.add(object.get(key).toString());
                    }
                }
                list.add(tmp);
            }
        }
        JSqlMapData.loadSchema(tabSchema, tableName, list);
        Class.forName("org.apache.calcite.jdbc.Driver");
        Properties info = new Properties();
        Connection connection = null;
        ResultSet result2 = null;
        Statement st = null;
        List<Map<String, Object>> ret = null;
        try {
            System.out.println(file.getAbsolutePath());
            connection = DriverManager.getConnection("jdbc:calcite:model=" + file.getAbsolutePath(), info);
            st = connection.createStatement();
            result2 = st.executeQuery(sql);
            ResultSetMetaData rsmd = result2.getMetaData();
            ret = new ArrayList<Map<String, Object>>();
            while (result2.next()) {
                Map<String, Object> map = new HashMap<String, Object>();
                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    map.put(rsmd.getColumnName(i), result2.getString(rsmd.getColumnName(i)));
                }
                ret.add(map);
            }
        } finally {
            if (result2 != null) {
                result2.close();
            }
            if (st != null) {
                st.close();
            }
            if (connection != null) {
                connection.close();
            }
        }
        return JsonUtil.toJson(ret);
    }

    private static File createTempJson() throws IOException {
        Map<String, Object> object = new HashMap<>();
        object.put("version", "2.0");
        object.put("defaultSchema", "db");
        List<Map<String, Object>> array = new ArrayList<>();
        Map tmp = new HashMap<String, Map<String, String>>();
        tmp.put("name", "db");
        tmp.put("type", "custom");
        tmp.put("factory", "hyren.serv6.b.realtimecollection.schema.JSqlSchemaFactory");
        Map tmp2 = new HashMap<>();
        tmp2.put("database", "calcite_memory_db");
        tmp.put("operand", tmp2);
        array.add(tmp);
        object.put("schemas", array);
        File f = File.createTempFile("calcitedb", ".json");
        FileWriter out = null;
        try {
            out = new FileWriter(f);
            out.write(JsonUtil.toJson(object));
        } finally {
            out.close();
        }
        f.deleteOnExit();
        return f;
    }
}
