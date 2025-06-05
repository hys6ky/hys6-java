package hyren.serv6.commons.utils.datatable;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.codes.IsFlag;
import hyren.serv6.base.exception.BusinessException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/3/30 0030 下午 03:23")
public class DataTableFieldUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "col_type", desc = "", range = "")
    @Return(desc = "", range = "")
    public static Map<String, String> parsingFiledType(String col_type) {
        Map<String, String> map = new HashMap<>();
        if (col_type.indexOf('(') != -1) {
            String data_type = col_type.substring(0, col_type.indexOf('('));
            map.put("data_type", data_type);
            String substring = col_type.substring(col_type.indexOf('(') + 1, col_type.lastIndexOf(')'));
            map.put("data_len", substring.split(",")[0]);
            if (substring.split(",").length == 1) {
                map.put("decimal_point", "0");
            } else {
                map.put("decimal_point", substring.split(",")[1]);
            }
        } else {
            map.put("data_type", col_type);
            map.put("data_len", "0");
            map.put("decimal_point", "0");
        }
        if (map.isEmpty()) {
            throw new BusinessException("字段类型解析失败!");
        }
        return map;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "table_column_list", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<Map<String, String>> metaInfoToList(List<Map<String, Object>> table_column_list) {
        List<Map<String, String>> column_list = new ArrayList<>();
        table_column_list.forEach(table_column -> {
            Map<String, String> map = new HashMap<>();
            map.put("column_id", table_column.get("column_id").toString());
            map.put("column_name", table_column.get("column_name").toString());
            map.put("column_ch_name", table_column.get("column_ch_name").toString());
            String column_type = table_column.get("column_type").toString();
            map.put("data_type", parsingFiledType(column_type).get("data_type").toLowerCase());
            map.put("data_len", parsingFiledType(column_type).get("data_len"));
            map.put("decimal_point", parsingFiledType(column_type).get("decimal_point"));
            String is_primary_key = table_column.get("is_primary_key").toString();
            if (StringUtil.isBlank(is_primary_key)) {
                is_primary_key = IsFlag.Fou.getCode();
            }
            map.put("is_primary_key", is_primary_key);
            column_list.add(map);
        });
        return column_list;
    }
}
