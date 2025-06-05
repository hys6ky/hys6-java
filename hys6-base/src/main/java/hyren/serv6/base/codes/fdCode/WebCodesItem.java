package hyren.serv6.base.codes.fdCode;

import fd.ng.db.resultset.Result;
import hyren.serv6.base.codes.CodesItem;
import hyren.serv6.base.exception.AppSystemException;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiImplicitParam;
import io.swagger.annotations.ApiImplicitParams;
import io.swagger.annotations.ApiOperation;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import java.io.ByteArrayInputStream;
import java.io.ObjectInputStream;
import java.lang.reflect.Method;
import java.util.*;

@Slf4j
@Api(tags = "")
@RestController()
@RequestMapping("/code")
@Configuration
public class WebCodesItem {

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "category", paramType = "query", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "value", paramType = "query", value = "", required = true, dataTypeClass = String.class) })
    @PostMapping("/getCode")
    public static String getCode(String category, String value) {
        Class<?> aClass = (Class<?>) CodesItem.mapCat.get(category);
        if (aClass == null)
            return "";
        Enum<?>[] enumsCode;
        Method values1;
        try {
            values1 = aClass.getMethod("values");
            Object aNull = values1.invoke("null");
            enumsCode = (Enum<?>[]) aNull;
            for (Enum<?> aClass1 : enumsCode) {
                Method getValue = aClass1.getClass().getMethod("getValue");
                Object valueName = getValue.invoke(aClass1);
                if (valueName.equals(value)) {
                    Method getCode = aClass1.getClass().getMethod("getCode");
                    Object code = getCode.invoke(aClass1);
                    return String.valueOf(code);
                }
            }
        } catch (Exception e) {
            throw new AppSystemException("根据分类编号" + category + "和代码项值" + value + "没有找到对应的代码项", e);
        }
        return "";
    }

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "category", paramType = "query", value = "", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "code", paramType = "query", value = "", required = true, dataTypeClass = String.class) })
    @PostMapping("/getValue")
    public static String getValue(String category, String code) {
        Class<?> aClass = (Class<?>) CodesItem.mapCat.get(category);
        if (aClass == null)
            return "";
        Object invoke;
        try {
            Method ofValueByCode = aClass.getMethod("ofValueByCode", String.class);
            invoke = ofValueByCode.invoke(null, code);
        } catch (Exception e) {
            throw new AppSystemException("根据分类编号" + category + "和代码项" + code + "没有找到对应的代码项", e);
        }
        return (invoke == null) ? "" : invoke.toString();
    }

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "category", value = "", paramType = "query", required = true, dataTypeClass = String.class) })
    @PostMapping("/getCategoryItems")
    public static Result getCategoryItems(String category) {
        Class<?> aClass = (Class<?>) CodesItem.mapCat.get(category);
        Enum<?>[] aa;
        Result rs = new Result();
        List<Map<String, Object>> results = new ArrayList<>();
        try {
            Method values1 = aClass.getMethod("values");
            Object aNull = values1.invoke("null");
            aa = (Enum<?>[]) aNull;
            for (Enum<?> aClass1 : aa) {
                Map<String, Object> map = new HashMap<>();
                Method getCode = aClass1.getClass().getMethod("getCode");
                Object code = getCode.invoke(aClass1);
                map.put("code", code);
                Method getValue = aClass1.getClass().getMethod("getValue");
                Object value = getValue.invoke(aClass1);
                map.put("value", value);
                Method getCatCode = aClass1.getClass().getMethod("getCatCode");
                Object catCode = getCatCode.invoke(aClass1);
                map.put("catCode", catCode);
                Method getCatValue = aClass1.getClass().getMethod("getCatValue");
                Object catValue = getCatValue.invoke(aClass1);
                map.put("catValue", catValue);
                results.add(map);
            }
            rs.add(results);
        } catch (Exception e) {
            throw new AppSystemException("根据" + category + "没有找到对应的代码项", e);
        }
        return rs;
    }

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "category", value = "", paramType = "query", required = true, dataTypeClass = String.class) })
    @PostMapping("/getCodeItems")
    public static Map<String, String> getCodeItems(String category) {
        Class<?> aClass = (Class<?>) CodesItem.mapCat.get(category);
        Enum<?>[] aa;
        Map<String, String> map = new HashMap<>();
        try {
            Method values1 = aClass.getMethod("values");
            Object aNull = values1.invoke("null");
            aa = (Enum<?>[]) aNull;
            for (Enum<?> aClass1 : aa) {
                Method getCode = aClass1.getClass().getMethod("getCode");
                String code = getCode.invoke(aClass1).toString();
                map.put(aClass1.name(), code);
            }
        } catch (Exception e) {
            throw new AppSystemException("根据" + category + "没有找到对应的代码项", e);
        }
        return map;
    }

    @ApiOperation(value = "")
    @GetMapping("/getAllCodeItems")
    public static Map<String, Object> getAllCodeItems() {
        Map<String, Object> map = new HashMap<>();
        Set<Map.Entry<String, Object>> entries = CodesItem.mapCat.entrySet();
        for (Map.Entry<String, Object> entry : entries) {
            String key = entry.getKey();
            Result rs = getCategoryItems(key);
            List<Map<String, Object>> maps = rs.toList();
            map.put(key, maps);
        }
        return map;
    }

    @ApiOperation(value = "")
    @ApiImplicitParams({ @ApiImplicitParam(name = "category", value = "", paramType = "query", required = true, dataTypeClass = String.class), @ApiImplicitParam(name = "obj", value = "", paramType = "query", required = true, dataTypeClass = String.class) })
    @PostMapping("/putCode")
    public static void putCode(String category, String obj) {
        byte[] data = Base64.getDecoder().decode(obj);
        try (ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data))) {
            Object o = ois.readObject();
            Class<?> aClass = (Class<?>) o;
            CodesItem.mapCat.put(category, aClass);
        } catch (Exception e) {
            throw new AppSystemException(e);
        }
    }
}
