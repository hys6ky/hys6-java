package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum DFType {

    JiangGuan("1", "监管补录", "109", "补录类型"), ChangGui("2", "常规补录", "109", "补录类型"), LinShi("3", "临时补录", "109", "补录类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    DFType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "DFType";

    public static String ofValueByCode(String code) {
        for (DFType typeCode : DFType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DFType:补录类型]");
    }

    public static DFType ofEnumByCode(String code) {
        for (DFType typeCode : DFType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DFType:补录类型]");
    }

    public static String getCodeByValue(String value) {
        for (DFType typeCode : DFType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[DFType:补录类型]");
    }

    public static String ofCatValue() {
        return DFType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DFType.values()[0].getCatCode();
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
