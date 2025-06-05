package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum AuthType {

    YunXu("1", "允许", "58", "权限类型"), BuYunXu("2", "不允许", "58", "权限类型"), YiCi("3", "一次", "58", "权限类型"), ShenQing("0", "申请", "58", "权限类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AuthType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "AuthType";

    public static String ofValueByCode(String code) {
        for (AuthType typeCode : AuthType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AuthType:权限类型]");
    }

    public static AuthType ofEnumByCode(String code) {
        for (AuthType typeCode : AuthType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[AuthType:权限类型]");
    }

    public static String getCodeByValue(String value) {
        for (AuthType typeCode : AuthType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[AuthType:权限类型]");
    }

    public static String ofCatValue() {
        return AuthType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AuthType.values()[0].getCatCode();
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
