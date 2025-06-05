package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum HdfsFileType {

    Csv("1", "csv", "81", "hdfs文件类型"),
    Parquet("2", "parquet", "81", "hdfs文件类型"),
    Avro("3", "avro", "81", "hdfs文件类型"),
    OrcFile("4", "orcfile", "81", "hdfs文件类型"),
    SequenceFile("5", "sequencefile", "81", "hdfs文件类型"),
    Other("6", "其他", "81", "hdfs文件类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    HdfsFileType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "HdfsFileType";

    public static String ofValueByCode(String code) {
        for (HdfsFileType typeCode : HdfsFileType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[HdfsFileType:hdfs文件类型]");
    }

    public static HdfsFileType ofEnumByCode(String code) {
        for (HdfsFileType typeCode : HdfsFileType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[HdfsFileType:hdfs文件类型]");
    }

    public static String getCodeByValue(String value) {
        for (HdfsFileType typeCode : HdfsFileType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[HdfsFileType:hdfs文件类型]");
    }

    public static String ofCatValue() {
        return HdfsFileType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return HdfsFileType.values()[0].getCatCode();
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
