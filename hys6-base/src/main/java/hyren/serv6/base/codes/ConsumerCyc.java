package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum ConsumerCyc {

    WuXianQi("1", "无限期", "123", "消费周期"), AnShiJianJieShu("2", "按时间结束", "123", "消费周期"), AnShuJuLiangJieShu("3", "按数据量结束", "123", "消费周期");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ConsumerCyc(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ConsumerCyc";

    public static String ofValueByCode(String code) {
        for (ConsumerCyc typeCode : ConsumerCyc.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ConsumerCyc:消费周期]");
    }

    public static ConsumerCyc ofEnumByCode(String code) {
        for (ConsumerCyc typeCode : ConsumerCyc.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ConsumerCyc:消费周期]");
    }

    public static String getCodeByValue(String value) {
        for (ConsumerCyc typeCode : ConsumerCyc.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[ConsumerCyc:消费周期]");
    }

    public static String ofCatValue() {
        return ConsumerCyc.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ConsumerCyc.values()[0].getCatCode();
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
