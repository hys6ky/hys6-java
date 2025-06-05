package hyren.serv6.c.util;

import fd.ng.core.utils.JsonUtil;
import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MapUtil {

    public static <T> List<T> getObjectListToMapList(List<Map> list, Class<T> type) {
        if (list != null) {
            return list.stream().map(map -> {
                Object t = fd.ng.core.utils.MapUtil.toObject(map, type);
                if (type.isInstance(t)) {
                    return type.cast(t);
                }
                return null;
            }).collect(Collectors.toList());
        }
        return null;
    }

    public static <T> List<T> getObjectList(String str, Class<T> type) {
        try {
            List<Map> list = JsonUtil.toObject(str, List.class);
            return MapUtil.getObjectListToMapList(list, type);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K, V> V get(Map<K, V> map, K key) {
        if (map != null) {
            return map.get(key);
        }
        return null;
    }

    public static <K, V> V get(Map<K, V> map, K key, V defaultValue) {
        if (map != null) {
            if (map.get(key) != null) {
                return map.get(key);
            }
        }
        return defaultValue;
    }

    public static <K, V> V get(Map<K, V> map, K key, Class<V> clazz) {
        if (map != null) {
            if (map.get(key) != null && clazz.isInstance(map.get(key))) {
                return clazz.cast(map.get(key));
            }
        }
        try {
            Constructor<V> constructor = clazz.getDeclaredConstructor(String.class);
            V newInstance = constructor.newInstance(map.get(key).toString());
            return newInstance;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    public static <K, V> V get(Map<K, V> map, K key, Class<V> clazz, V defaultValue) {
        if (map != null && map.get(key) != null) {
            if (clazz.isInstance(map.get(key))) {
                return clazz.cast(map.get(key));
            }
            try {
                Constructor<V> constructor = clazz.getDeclaredConstructor(String.class);
                V newObject = constructor.newInstance(map.get(key).toString());
                return newObject;
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return defaultValue;
    }
}
