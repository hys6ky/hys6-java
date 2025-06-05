package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum DqcExecMode {

    ShouGong("MAN", "手工", "80", "数据质量执行方式"), ZiDong("AUTO", "自动", "80", "数据质量执行方式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    DqcExecMode(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "DqcExecMode";

    public static String ofValueByCode(String code) {
        for (DqcExecMode typeCode : DqcExecMode.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DqcExecMode:数据质量执行方式]");
    }

    public static DqcExecMode ofEnumByCode(String code) {
        for (DqcExecMode typeCode : DqcExecMode.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DqcExecMode:数据质量执行方式]");
    }

    public static String getCodeByValue(String value) {
        for (DqcExecMode typeCode : DqcExecMode.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[DqcExecMode:数据质量执行方式]");
    }

    public static String ofCatValue() {
        return DqcExecMode.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DqcExecMode.values()[0].getCatCode();
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
