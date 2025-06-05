package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum AutoValueType {

    ZiFuChuan("01", "字符串", "119", "值类型"), ShuZhi("02", "数值", "119", "值类型"), RiQi("03", "日期", "119", "值类型"), MeiJu("04", "枚举", "119", "值类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AutoValueType(String code, String value, String catCode, String catValue) {
        this.code = code;
        this.value = value;
        this.catCode = catCode;
        this.catValue = catValue;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    public String getCatCode() {
        return catCode;
    }

    public String getCatValue() {
        return catValue;
    }

    public static final String CodeName = "AutoValueType";

    public static String ofValueByCode(String code) {
        for (AutoValueType typeCode : AutoValueType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoValueType:值类型]");
    }

    public static AutoValueType ofEnumByCode(String code) {
        for (AutoValueType typeCode : AutoValueType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AutoValueType:值类型]");
    }

    public static String getCodeByValue(String value) {
        for (AutoValueType typeCode : AutoValueType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[AutoValueType:值类型]");
    }

    public static String ofCatValue() {
        return AutoValueType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AutoValueType.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemRuntimeException("There‘s no need for you to !");
    }

    public static String Serialized() {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(HdfsFileType.class);
            String obj = Base64.getEncoder().encodeToString(baos.toByteArray());
            return obj;
        } catch (Exception e) {
            throw new SystemRuntimeException(e);
        }
    }
}
