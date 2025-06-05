package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum PmpState {

    WeiXiangYing("1", "未响应", "996", "项目状态"),
    YiXiangYing("2", "已响应", "996", "项目状态"),
    WeiFaBu("3", "未发布", "996", "项目状态"),
    YiFaBu("4", "已发布", "996", "项目状态"),
    WeiDaBao("5", "未打包", "996", "项目状态"),
    YiDaBao("6", "已打包", "996", "项目状态"),
    YiShangXian("7", "已上线", "996", "项目状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    PmpState(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "PmpState";

    public static String ofValueByCode(String code) {
        for (PmpState typeCode : PmpState.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[PmpState:项目状态]");
    }

    public static PmpState ofEnumByCode(String code) {
        for (PmpState typeCode : PmpState.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[PmpState:项目状态]");
    }

    public static String getCodeByValue(String value) {
        for (PmpState typeCode : PmpState.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[PmpState:项目状态]");
    }

    public static String ofCatValue() {
        return PmpState.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return PmpState.values()[0].getCatCode();
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
