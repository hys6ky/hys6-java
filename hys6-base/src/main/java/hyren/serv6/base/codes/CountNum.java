package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum CountNum {

    YiWan("10000", "1万左右", "43", "记录总数"),
    ShiWan("100000", "10万左右", "43", "记录总数"),
    BaiWan("1000000", "100万左右", "43", "记录总数"),
    Qianwan("10000000", "1000万左右", "43", "记录总数"),
    Yi("100000000", "亿左右", "43", "记录总数"),
    YiYiShang("100000001", "亿以上", "43", "记录总数");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    CountNum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "CountNum";

    public static String ofValueByCode(String code) {
        for (CountNum typeCode : CountNum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[CountNum:记录总数]");
    }

    public static CountNum ofEnumByCode(String code) {
        for (CountNum typeCode : CountNum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[CountNum:记录总数]");
    }

    public static String getCodeByValue(String value) {
        for (CountNum typeCode : CountNum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[CountNum:记录总数]");
    }

    public static String ofCatValue() {
        return CountNum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return CountNum.values()[0].getCatCode();
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
