package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum Dispatch_Frequency {

    DAILY("D", "天(D)", "22", "ETL调度频率"),
    MONTHLY("M", "月(M)", "22", "ETL调度频率"),
    WEEKLY("W", "周(W)", "22", "ETL调度频率"),
    TENDAYS("X", "旬(X)", "22", "ETL调度频率"),
    YEARLY("Y", "年(Y)", "22", "ETL调度频率"),
    PinLv("F", "频率(F)", "22", "ETL调度频率");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    Dispatch_Frequency(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "Dispatch_Frequency";

    public static String ofValueByCode(String code) {
        for (Dispatch_Frequency typeCode : Dispatch_Frequency.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Dispatch_Frequency:ETL调度频率]");
    }

    public static Dispatch_Frequency ofEnumByCode(String code) {
        for (Dispatch_Frequency typeCode : Dispatch_Frequency.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Dispatch_Frequency:ETL调度频率]");
    }

    public static String getCodeByValue(String value) {
        for (Dispatch_Frequency typeCode : Dispatch_Frequency.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[Dispatch_Frequency:ETL调度频率]");
    }

    public static String ofCatValue() {
        return Dispatch_Frequency.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return Dispatch_Frequency.values()[0].getCatCode();
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
