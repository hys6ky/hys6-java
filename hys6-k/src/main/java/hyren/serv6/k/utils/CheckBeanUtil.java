package hyren.serv6.k.utils;

import fd.ng.core.annotation.DocClass;
import fd.ng.core.annotation.Method;
import fd.ng.core.annotation.Param;
import fd.ng.core.annotation.Return;
import hyren.daos.bizpot.commons.Dbo;
import hyren.serv6.base.exception.BusinessException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@DocClass(desc = "", author = "BY-HLL", createdate = "2020/4/10 0010 上午 10:43")
public class CheckBeanUtil {

    @Method(desc = "", logicStep = "")
    @Param(name = "object", desc = "", range = "")
    @Return(desc = "", range = "")
    public static boolean checkFullNull(Object object) {
        java.lang.reflect.Method[] methods = object.getClass().getMethods();
        for (java.lang.reflect.Method method : methods) {
            String methodName = method.getName();
            if (methodName.startsWith("get") && !methodName.equals("getClass")) {
                try {
                    Object check = method.invoke(object);
                    if (null != check) {
                        return false;
                    }
                } catch (IllegalArgumentException | IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }
        return true;
    }

    @Method(desc = "", logicStep = "")
    @Param(name = "object", desc = "", range = "")
    @Param(name = "regEx", desc = "", range = "")
    @Return(desc = "", range = "")
    public static List<String> getBeanValueWithPattern(Object object, String regEx) {
        List<String> str_s = new ArrayList<>();
        java.lang.reflect.Method[] methods = object.getClass().getMethods();
        for (java.lang.reflect.Method method : methods) {
            String methodName = method.getName();
            if (methodName.startsWith("get") && !methodName.equals("getClass")) {
                try {
                    Object check = method.invoke(object);
                    if (null != check) {
                        Pattern pattern = Pattern.compile(regEx);
                        Matcher matcher = pattern.matcher(check.toString());
                        while (matcher.find()) {
                            str_s.add(matcher.group());
                        }
                    }
                } catch (IllegalAccessException | InvocationTargetException e) {
                    e.printStackTrace();
                    throw new BusinessException("通过正则表达式提取字符串数组失败!");
                }
            }
        }
        return str_s;
    }

    public static boolean checkUserRole(long role_id, long menu_id) {
        OptionalLong aLong = Dbo.queryNumber("select count(*) from role_menu where role_id=? and menu_id=?", role_id, menu_id);
        if (aLong.isPresent()) {
            return aLong.getAsLong() > 0;
        } else {
            throw new BusinessException("检查用户菜单权限的SQL执行结果不合法!");
        }
    }
}
