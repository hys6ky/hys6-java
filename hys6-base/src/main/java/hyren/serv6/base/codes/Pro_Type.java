package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum Pro_Type {

    SHELL("SHELL", "SHELL", "20", "ETL作业类型"),
    PERL("PERL", "PERL", "20", "ETL作业类型"),
    BAT("BAT", "BAT", "20", "ETL作业类型"),
    JAVA("JAVA", "JAVA", "20", "ETL作业类型"),
    PYTHON("PYTHON", "PYTHON", "20", "ETL作业类型"),
    WF("WF", "WF", "20", "ETL作业类型"),
    KETTLETRSN("KETTLETRSN", "KETTLETRSN", "20", "ETL作业类型"),
    KETTLEJOB("KETTLEJOB", "KETTLEJOB", "20", "ETL作业类型"),
    Yarn("Yarn", "Yarn", "20", "ETL作业类型"),
    Thrift("Thrift", "Thrift", "20", "ETL作业类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    Pro_Type(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "Pro_Type";

    public static String ofValueByCode(String code) {
        for (Pro_Type typeCode : Pro_Type.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Pro_Type:ETL作业类型]");
    }

    public static Pro_Type ofEnumByCode(String code) {
        for (Pro_Type typeCode : Pro_Type.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Pro_Type:ETL作业类型]");
    }

    public static String getCodeByValue(String value) {
        for (Pro_Type typeCode : Pro_Type.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[Pro_Type:ETL作业类型]");
    }

    public static String ofCatValue() {
        return Pro_Type.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return Pro_Type.values()[0].getCatCode();
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
