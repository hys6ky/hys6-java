package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum Meddle_type {

    GRP_RESUME("GR", "分组级续跑", "29", "ETL干预类型"),
    GRP_PAUSE("GP", "分组级暂停", "29", "ETL干预类型"),
    GRP_ORIGINAL("GO", "分组级重跑，从源头开始", "29", "ETL干预类型"),
    JOB_TRIGGER("JT", "作业直接跑", "29", "ETL干预类型"),
    JOB_STOP("JS", "作业停止", "29", "ETL干预类型"),
    JOB_RERUN("JR", "作业重跑", "29", "ETL干预类型"),
    JOB_PRIORITY("JP", "作业临时调整优先级", "29", "ETL干预类型"),
    JOB_JUMP("JJ", "作业跳过", "29", "ETL干预类型"),
    SYS_SHIFT("SF", "系统日切", "29", "ETL干预类型"),
    SYS_STOP("SS", "系统停止", "29", "ETL干预类型"),
    SYS_PAUSE("SP", "系统级暂停", "29", "ETL干预类型"),
    SYS_ORIGINAL("SO", "系统级重跑，从源头开始", "29", "ETL干预类型"),
    SYS_RESUME("SR", "系统级续跑", "29", "ETL干预类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    Meddle_type(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "Meddle_type";

    public static String ofValueByCode(String code) {
        for (Meddle_type typeCode : Meddle_type.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Meddle_type:ETL干预类型]");
    }

    public static Meddle_type ofEnumByCode(String code) {
        for (Meddle_type typeCode : Meddle_type.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Meddle_type:ETL干预类型]");
    }

    public static String getCodeByValue(String value) {
        for (Meddle_type typeCode : Meddle_type.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[Meddle_type:ETL干预类型]");
    }

    public static String ofCatValue() {
        return Meddle_type.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return Meddle_type.values()[0].getCatCode();
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
