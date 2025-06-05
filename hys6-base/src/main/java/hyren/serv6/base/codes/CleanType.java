package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum CleanType {

    ZiFuBuQi("1", "字符补齐", "54", "清洗方式"),
    ZiFuTiHuan("2", "字符替换", "54", "清洗方式"),
    ShiJianZhuanHuan("3", "时间转换", "54", "清洗方式"),
    MaZhiZhuanHuan("4", "码值转换", "54", "清洗方式"),
    ZiFuHeBing("5", "字符合并", "54", "清洗方式"),
    ZiFuChaiFen("6", "字符拆分", "54", "清洗方式"),
    ZiFuTrim("7", "字符trim", "54", "清洗方式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    CleanType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "CleanType";

    public static String ofValueByCode(String code) {
        for (CleanType typeCode : CleanType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[CleanType:清洗方式]");
    }

    public static CleanType ofEnumByCode(String code) {
        for (CleanType typeCode : CleanType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[CleanType:清洗方式]");
    }

    public static String getCodeByValue(String value) {
        for (CleanType typeCode : CleanType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[CleanType:清洗方式]");
    }

    public static String ofCatValue() {
        return CleanType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return CleanType.values()[0].getCatCode();
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
