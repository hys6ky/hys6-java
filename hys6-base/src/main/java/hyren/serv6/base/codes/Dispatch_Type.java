package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum Dispatch_Type {

    BEFORE("B", "批前(B)", "23", "ETL调度类型"), DEPENDENCE("D", "依赖触发(D)", "23", "ETL调度类型"), TPLUS1("T", "定时T+1触发(T)", "23", "ETL调度类型"), TPLUS0("Z", "定时T+0触发(Z)", "23", "ETL调度类型"), AFTER("A", "批后(A)", "23", "ETL调度类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    Dispatch_Type(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "Dispatch_Type";

    public static String ofValueByCode(String code) {
        for (Dispatch_Type typeCode : Dispatch_Type.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Dispatch_Type:ETL调度类型]");
    }

    public static Dispatch_Type ofEnumByCode(String code) {
        for (Dispatch_Type typeCode : Dispatch_Type.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Dispatch_Type:ETL调度类型]");
    }

    public static String getCodeByValue(String value) {
        for (Dispatch_Type typeCode : Dispatch_Type.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[Dispatch_Type:ETL调度类型]");
    }

    public static String ofCatValue() {
        return Dispatch_Type.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return Dispatch_Type.values()[0].getCatCode();
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
