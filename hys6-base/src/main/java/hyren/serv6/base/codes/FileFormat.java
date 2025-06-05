package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum FileFormat {

    DingChang("0", "定长", "44", "DB文件格式"),
    FeiDingChang("1", "非定长", "44", "DB文件格式"),
    CSV("2", "CSV", "44", "DB文件格式"),
    SEQUENCEFILE("3", "SEQUENCEFILE", "44", "DB文件格式"),
    PARQUET("4", "PARQUET", "44", "DB文件格式"),
    ORC("5", "ORC", "44", "DB文件格式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    FileFormat(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "FileFormat";

    public static String ofValueByCode(String code) {
        for (FileFormat typeCode : FileFormat.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[FileFormat:DB文件格式]");
    }

    public static FileFormat ofEnumByCode(String code) {
        for (FileFormat typeCode : FileFormat.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[FileFormat:DB文件格式]");
    }

    public static String getCodeByValue(String value) {
        for (FileFormat typeCode : FileFormat.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[FileFormat:DB文件格式]");
    }

    public static String ofCatValue() {
        return FileFormat.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return FileFormat.values()[0].getCatCode();
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
