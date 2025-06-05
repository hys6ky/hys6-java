package hyren.serv6.commons.utils.constant;

import fd.ng.core.utils.StringUtil;
import fd.ng.db.jdbc.DatabaseWrapper;
import fd.ng.db.jdbc.SqlOperator;
import hyren.serv6.base.entity.SysPara;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PropertyParaValue {

    private static Map<String, String> mapParaType;

    static {
        mapParaType = new HashMap<>();
        try (DatabaseWrapper db = new DatabaseWrapper()) {
            List<SysPara> sys_paras = SqlOperator.queryList(db, SysPara.class, "select para_name,para_value from " + SysPara.TableName);
            for (SysPara sys_para : sys_paras) {
                String para_name = sys_para.getPara_name();
                String para_value = sys_para.getPara_value();
                mapParaType.put(para_name, para_value);
            }
        }
    }

    public static String getString(String name, String defaultValue) {
        try {
            return !StringUtil.isEmpty(mapParaType.get(name)) ? mapParaType.get(name) : defaultValue;
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static int getInt(String name, int defaultValue) {
        try {
            return Integer.parseInt(mapParaType.get(name));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static long getLong(String name, long defaultValue) {
        try {
            return Long.parseLong(mapParaType.get(name));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static float getFloat(String name, float defaultValue) {
        try {
            return Double.valueOf(mapParaType.get(name)).floatValue();
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static double getDouble(String name, double defaultValue) {
        try {
            return Double.parseDouble(mapParaType.get(name));
        } catch (Exception e) {
            return defaultValue;
        }
    }

    public static Boolean getBoolean(String name, boolean defaultValue) {
        return StringUtil.isBlank(mapParaType.get(name)) ? defaultValue : Boolean.parseBoolean(mapParaType.get(name));
    }
}
