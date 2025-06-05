package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum Job_Status {

    DONE("D", "完成", "25", "ETL作业状态"),
    ERROR("E", "错误", "25", "ETL作业状态"),
    PENDING("P", "挂起", "25", "ETL作业状态"),
    RUNNING("R", "运行", "25", "ETL作业状态"),
    STOP("S", "停止", "25", "ETL作业状态"),
    WAITING("W", "等待", "25", "ETL作业状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    Job_Status(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "Job_Status";

    public static String ofValueByCode(String code) {
        for (Job_Status typeCode : Job_Status.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Job_Status:ETL作业状态]");
    }

    public static Job_Status ofEnumByCode(String code) {
        for (Job_Status typeCode : Job_Status.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Job_Status:ETL作业状态]");
    }

    public static String getCodeByValue(String value) {
        for (Job_Status typeCode : Job_Status.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[Job_Status:ETL作业状态]");
    }

    public static String ofCatValue() {
        return Job_Status.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return Job_Status.values()[0].getCatCode();
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
