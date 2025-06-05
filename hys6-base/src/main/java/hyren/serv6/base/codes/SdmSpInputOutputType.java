package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SdmSpInputOutputType {

    WENBENWENJIAN("1", "文本文件", "133", "StreamingPro输入输出的类型"), SHUJUKUBIAO("2", "存储目的", "133", "StreamingPro输入输出的类型"), XIAOFEIZHUTI("3", "消费主题", "133", "StreamingPro输入输出的类型"), REST("5", "RESRT服务", "133", "StreamingPro输入输出的类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SdmSpInputOutputType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SdmSpInputOutputType";

    public static String ofValueByCode(String code) {
        for (SdmSpInputOutputType typeCode : SdmSpInputOutputType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmSpInputOutputType:StreamingPro输入输出的类型]");
    }

    public static SdmSpInputOutputType ofEnumByCode(String code) {
        for (SdmSpInputOutputType typeCode : SdmSpInputOutputType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmSpInputOutputType:StreamingPro输入输出的类型]");
    }

    public static String getCodeByValue(String value) {
        for (SdmSpInputOutputType typeCode : SdmSpInputOutputType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SdmSpInputOutputType:StreamingPro输入输出的类型]");
    }

    public static String ofCatValue() {
        return SdmSpInputOutputType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SdmSpInputOutputType.values()[0].getCatCode();
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
