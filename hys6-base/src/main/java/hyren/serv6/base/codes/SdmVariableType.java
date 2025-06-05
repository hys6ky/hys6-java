package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SdmVariableType {

    ZhengShu("1", "整数", "128", "流数据管理变量类型"), ZiFuChuan("2", "字符串", "128", "流数据管理变量类型"), FuDianShu("3", "浮点数", "128", "流数据管理变量类型"), ZiJieShuZu("4", "字节数组", "128", "流数据管理变量类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SdmVariableType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SdmVariableType";

    public static String ofValueByCode(String code) {
        for (SdmVariableType typeCode : SdmVariableType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmVariableType:流数据管理变量类型]");
    }

    public static SdmVariableType ofEnumByCode(String code) {
        for (SdmVariableType typeCode : SdmVariableType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmVariableType:流数据管理变量类型]");
    }

    public static String getCodeByValue(String value) {
        for (SdmVariableType typeCode : SdmVariableType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SdmVariableType:流数据管理变量类型]");
    }

    public static String ofCatValue() {
        return SdmVariableType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SdmVariableType.values()[0].getCatCode();
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
