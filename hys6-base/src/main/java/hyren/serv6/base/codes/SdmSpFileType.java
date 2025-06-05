package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SdmSpFileType {

    CSV("1", "Csv", "138", "StreamingPro文本文件格式"), PARQUENT("2", "Parquent", "138", "StreamingPro文本文件格式"), JSON("3", "Json", "138", "StreamingPro文本文件格式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SdmSpFileType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SdmSpFileType";

    public static String ofValueByCode(String code) {
        for (SdmSpFileType typeCode : SdmSpFileType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmSpFileType:StreamingPro文本文件格式]");
    }

    public static SdmSpFileType ofEnumByCode(String code) {
        for (SdmSpFileType typeCode : SdmSpFileType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmSpFileType:StreamingPro文本文件格式]");
    }

    public static String getCodeByValue(String value) {
        for (SdmSpFileType typeCode : SdmSpFileType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SdmSpFileType:StreamingPro文本文件格式]");
    }

    public static String ofCatValue() {
        return SdmSpFileType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SdmSpFileType.values()[0].getCatCode();
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
