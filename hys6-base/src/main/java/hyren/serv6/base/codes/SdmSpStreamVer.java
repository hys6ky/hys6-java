package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SdmSpStreamVer {

    KAFKA8("1", "kafka8", "140", "StreamingPro流式数据的版本"), KAFKA9("2", "kafka9", "140", "StreamingPro流式数据的版本"), KAFKA("3", "kafka", "140", "StreamingPro流式数据的版本");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SdmSpStreamVer(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SdmSpStreamVer";

    public static String ofValueByCode(String code) {
        for (SdmSpStreamVer typeCode : SdmSpStreamVer.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmSpStreamVer:StreamingPro流式数据的版本]");
    }

    public static SdmSpStreamVer ofEnumByCode(String code) {
        for (SdmSpStreamVer typeCode : SdmSpStreamVer.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmSpStreamVer:StreamingPro流式数据的版本]");
    }

    public static String getCodeByValue(String value) {
        for (SdmSpStreamVer typeCode : SdmSpStreamVer.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SdmSpStreamVer:StreamingPro流式数据的版本]");
    }

    public static String ofCatValue() {
        return SdmSpStreamVer.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SdmSpStreamVer.values()[0].getCatCode();
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
