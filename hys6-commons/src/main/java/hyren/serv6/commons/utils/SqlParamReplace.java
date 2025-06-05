package hyren.serv6.commons.utils;

import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.constant.Constant;
import lombok.extern.slf4j.Slf4j;
import java.util.List;

@Slf4j
public class SqlParamReplace {

    public static String replaceSqlParam(String collectSql, String sqlParam) {
        List<String> splitParamList = StringUtil.split(sqlParam, Constant.SQLDELIMITER);
        if (splitParamList != null && splitParamList.size() > 0) {
            log.debug("开始执行SQL {} , 参数替换: {}", collectSql, sqlParam);
            for (String splitParam : splitParamList) {
                List<String> key_value = StringUtil.split(splitParam, "=");
                String key = "#\\{" + key_value.get(0) + "}";
                String value = key_value.get(1);
                collectSql = collectSql.replaceAll(key, value);
            }
        }
        return collectSql;
    }
}
