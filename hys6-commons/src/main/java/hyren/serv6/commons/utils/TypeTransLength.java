package hyren.serv6.commons.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.base.exception.AppSystemException;
import hyren.serv6.commons.utils.datastorage.QueryLengthMapping;
import hyren.serv6.commons.utils.yaml.ConfFileLoader;
import hyren.serv6.commons.utils.yaml.YamlArray;
import hyren.serv6.commons.utils.yaml.YamlFactory;
import hyren.serv6.commons.utils.yaml.YamlMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@DocClass(desc = "", createdate = "2020/1/8 15:27", author = "zxz")
public class TypeTransLength {

    private static final Map<String, YamlMap> map = new HashMap<>();

    private static final String LKH = "(";

    private static final String RKH = ")";

    private static final String COMMA = ",";

    static {
        YamlMap rootConfig = YamlFactory.yaml2Map(ConfFileLoader.getConfFile("lengthMapping"));
        YamlArray arrays = rootConfig.getArray("defaultlengthmapping");
        for (int i = 0; i < arrays.size(); i++) {
            YamlMap trans = arrays.getMap(i);
            map.put(trans.getString("NAME"), trans);
        }
    }

    public static Integer getLength(String column_type) {
        column_type = column_type.toUpperCase().trim();
        if (column_type.contains(LKH) && column_type.contains(RKH)) {
            int start = column_type.indexOf(LKH);
            int end = column_type.indexOf(RKH);
            String substring = column_type.substring(start + 1, end);
            if (substring.contains(COMMA)) {
                List<String> split = StringUtil.split(substring, COMMA);
                return Integer.parseInt(split.get(0)) + 2;
            }
            return Integer.parseInt(substring);
        } else {
            return null;
        }
    }

    public static Integer getLength(String column_type, String database_type) {
        column_type = column_type.toUpperCase().trim();
        if (column_type.contains(LKH) && column_type.contains(RKH)) {
            int start = column_type.indexOf(LKH);
            int end = column_type.indexOf(RKH);
            String substring = column_type.substring(start + 1, end);
            if (substring.contains(COMMA)) {
                List<String> split = StringUtil.split(substring, COMMA);
                return Integer.parseInt(split.get(0)) + 2;
            }
            return Integer.parseInt(substring);
        } else {
            YamlMap yamlMap = map.get(database_type.toUpperCase());
            if (yamlMap == null) {
                throw new AppSystemException("数据库" + database_type + "的配置字段类型默认长度信息在Agent的" + QueryLengthMapping.CONF_FILE_NAME + "文件中没有，" + "请重新部署agent或者手动更新配置文件再重启Agent");
            }
            return Integer.parseInt(yamlMap.getString(column_type));
        }
    }
}
