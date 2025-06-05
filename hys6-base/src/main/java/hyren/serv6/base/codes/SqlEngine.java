package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SqlEngine {

    JDBC("1", "JDBC", "88", "sql执行引擎"), SPARK("2", "SPARK", "88", "sql执行引擎"), FLINK("3", "Flink Sql", "88", "sql执行引擎"), sTREAMING("4", "Spark Streaming", "88", "sql执行引擎");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SqlEngine(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SqlEngine";

    public static String ofValueByCode(String code) {
        for (SqlEngine typeCode : SqlEngine.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SqlEngine:sql执行引擎]");
    }

    public static SqlEngine ofEnumByCode(String code) {
        for (SqlEngine typeCode : SqlEngine.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SqlEngine:sql执行引擎]");
    }

    public static String getCodeByValue(String value) {
        for (SqlEngine typeCode : SqlEngine.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SqlEngine:sql执行引擎]");
    }

    public static String ofCatValue() {
        return SqlEngine.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SqlEngine.values()[0].getCatCode();
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
