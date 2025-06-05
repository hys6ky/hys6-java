package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum JobExecuteState {

    DengDai("100", "等待", "87", "作业运行状态"),
    YunXing("101", "运行", "87", "作业运行状态"),
    ZanTing("102", "暂停", "87", "作业运行状态"),
    ZhongZhi("103", "中止", "87", "作业运行状态"),
    WanCheng("104", "完成", "87", "作业运行状态"),
    ShiBai("105", "失败", "87", "作业运行状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    JobExecuteState(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "JobExecuteState";

    public static String ofValueByCode(String code) {
        for (JobExecuteState typeCode : JobExecuteState.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[JobExecuteState:作业运行状态]");
    }

    public static JobExecuteState ofEnumByCode(String code) {
        for (JobExecuteState typeCode : JobExecuteState.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[JobExecuteState:作业运行状态]");
    }

    public static String getCodeByValue(String value) {
        for (JobExecuteState typeCode : JobExecuteState.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[JobExecuteState:作业运行状态]");
    }

    public static String ofCatValue() {
        return JobExecuteState.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return JobExecuteState.values()[0].getCatCode();
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
