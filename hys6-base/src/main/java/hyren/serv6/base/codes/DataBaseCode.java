package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum DataBaseCode {

    UTF_8("1", "UTF-8", "42", "采集编码"), GBK("2", "GBK", "42", "采集编码"), UTF_16("3", "UTF-16", "42", "采集编码"), GB2312("4", "GB2312", "42", "采集编码"), ISO_8859_1("5", "ISO-8859-1", "42", "采集编码");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    DataBaseCode(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "DataBaseCode";

    public static String ofValueByCode(String code) {
        for (DataBaseCode typeCode : DataBaseCode.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DataBaseCode:采集编码]");
    }

    public static DataBaseCode ofEnumByCode(String code) {
        for (DataBaseCode typeCode : DataBaseCode.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DataBaseCode:采集编码]");
    }

    public static String getCodeByValue(String value) {
        for (DataBaseCode typeCode : DataBaseCode.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[DataBaseCode:采集编码]");
    }

    public static String ofCatValue() {
        return DataBaseCode.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DataBaseCode.values()[0].getCatCode();
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
