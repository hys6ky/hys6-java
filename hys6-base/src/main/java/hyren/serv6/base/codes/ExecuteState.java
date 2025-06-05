package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum ExecuteState {

    KaiShiYunXing("01", "开始运行", "39", "运行状态"),
    YunXingWanCheng("02", "运行完成", "39", "运行状态"),
    YunXingShiBai("99", "运行失败", "39", "运行状态"),
    TongZhiChengGong("20", "通知成功", "39", "运行状态"),
    TongZhiShiBai("21", "通知失败", "39", "运行状态"),
    ZanTingYunXing("30", "暂停运行", "39", "运行状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ExecuteState(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ExecuteState";

    public static String ofValueByCode(String code) {
        for (ExecuteState typeCode : ExecuteState.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ExecuteState:运行状态]");
    }

    public static ExecuteState ofEnumByCode(String code) {
        for (ExecuteState typeCode : ExecuteState.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ExecuteState:运行状态]");
    }

    public static String getCodeByValue(String value) {
        for (ExecuteState typeCode : ExecuteState.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[ExecuteState:运行状态]");
    }

    public static String ofCatValue() {
        return ExecuteState.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ExecuteState.values()[0].getCatCode();
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
