package hyren.serv6.k.utils.easyexcel;

import com.alibaba.excel.annotation.ExcelProperty;
import hyren.daos.base.exception.SystemBusinessException;
import org.apache.commons.lang3.StringUtils;
import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ExcelValidUtil {

    public static void valid(Object object) {
        Field[] fields = object.getClass().getDeclaredFields();
        for (Field field : fields) {
            field.setAccessible(true);
            Object fieldValue;
            try {
                fieldValue = field.get(object);
            } catch (IllegalAccessException e) {
                throw new RuntimeException(e);
            }
            if (null == fieldValue) {
                fieldValue = "";
            }
            boolean isExcelValid = field.isAnnotationPresent(ExcelValid.class);
            if (isExcelValid) {
                ExcelValid excelValid = field.getAnnotation(ExcelValid.class);
                ExcelProperty excelProperty = field.getAnnotation(ExcelProperty.class);
                String[] regulars = excelValid.regular();
                if (regulars.length == 0) {
                    regulars = Arrays.stream(excelValid.rule()).map(ValidRuleEnum::getRegular).collect(Collectors.toList()).toArray(new String[0]);
                }
                String msg = excelValid.message();
                StringBuilder newMsg = new StringBuilder();
                boolean isReal = true;
                for (String regular : regulars) {
                    Pattern pattern = Pattern.compile(regular);
                    if (!pattern.matcher(fieldValue.toString()).find()) {
                        isReal = false;
                        if (org.apache.commons.lang3.StringUtils.isBlank(msg)) {
                            List<String> msgList = Arrays.stream(excelValid.rule()).map(ValidRuleEnum::getMsg).collect(Collectors.toList());
                            newMsg.append(String.join(";", msgList));
                        }
                    }
                }
                if (org.apache.commons.lang3.StringUtils.isBlank(msg)) {
                    msg = String.join("-", excelProperty.value()) + " : " + newMsg;
                }
                if (!isReal) {
                    throw new SystemBusinessException(msg);
                }
            }
        }
    }

    public static void main(String[] args) {
        Pattern pattern = Pattern.compile("^\\S+$");
        pattern.matcher("");
    }
}
