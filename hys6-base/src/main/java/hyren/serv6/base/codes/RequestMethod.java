package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum RequestMethod {

    GET("1", "get", "", "接口请求方式"), POST("2", "post", "", "接口请求方式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    RequestMethod(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "RequestMethod";

    public static String ofValueByCode(String code) {
        for (RequestMethod typeCode : RequestMethod.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[RequestMethod:接口请求方式]");
    }

    public static RequestMethod ofEnumByCode(String code) {
        for (RequestMethod typeCode : RequestMethod.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[RequestMethod:接口请求方式]");
    }

    public static String getCodeByValue(String value) {
        for (RequestMethod typeCode : RequestMethod.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[RequestMethod:接口请求方式]");
    }

    public static String ofCatValue() {
        return RequestMethod.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return RequestMethod.values()[0].getCatCode();
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
