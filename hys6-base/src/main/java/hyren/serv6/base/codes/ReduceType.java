package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum ReduceType {

    TAR("1", "tar", "45", "压缩格式"), GZ("2", "gz", "45", "压缩格式"), ZIP("3", "zip", "45", "压缩格式"), NONE("4", "none", "45", "压缩格式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ReduceType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ReduceType";

    public static String ofValueByCode(String code) {
        for (ReduceType typeCode : ReduceType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ReduceType:压缩格式]");
    }

    public static ReduceType ofEnumByCode(String code) {
        for (ReduceType typeCode : ReduceType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[ReduceType:压缩格式]");
    }

    public static String getCodeByValue(String value) {
        for (ReduceType typeCode : ReduceType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[ReduceType:压缩格式]");
    }

    public static String ofCatValue() {
        return ReduceType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ReduceType.values()[0].getCatCode();
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
