package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum SdmTimestampFormat {

    ISO("1", "iso", "146", "流数据管理druid时间戳格式"), Millis("2", "millis", "146", "流数据管理druid时间戳格式"), Posix("3", "posix", "146", "流数据管理druid时间戳格式"), JodaTime("4", "jodaTime", "146", "流数据管理druid时间戳格式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    SdmTimestampFormat(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "SdmTimestampFormat";

    public static String ofValueByCode(String code) {
        for (SdmTimestampFormat typeCode : SdmTimestampFormat.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmTimestampFormat:流数据管理druid时间戳格式]");
    }

    public static SdmTimestampFormat ofEnumByCode(String code) {
        for (SdmTimestampFormat typeCode : SdmTimestampFormat.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[SdmTimestampFormat:流数据管理druid时间戳格式]");
    }

    public static String getCodeByValue(String value) {
        for (SdmTimestampFormat typeCode : SdmTimestampFormat.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[SdmTimestampFormat:流数据管理druid时间戳格式]");
    }

    public static String ofCatValue() {
        return SdmTimestampFormat.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return SdmTimestampFormat.values()[0].getCatCode();
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
