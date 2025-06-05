package hyren.serv6.c.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.springframework.util.CollectionUtils;

public class JsonUtil {

    public static <T> List<T> toListSafety(String json, Class<T> clazz) {
        List<Map> listMap = fd.ng.core.utils.JsonUtil.toObjectSafety(json, List.class).orElse(null);
        if (CollectionUtils.isEmpty(listMap)) {
            return new ArrayList<>();
        }
        return MapUtil.getObjectListToMapList(listMap, clazz);
    }
}
