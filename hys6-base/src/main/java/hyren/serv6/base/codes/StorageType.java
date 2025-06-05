package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum StorageType {

    TiHuan("3", "F1:替换", "41", "进数方式"),
    LiShiLaLian("6", "F3:历史拉链", "41", "进数方式"),
    QuanLiang("4", "F5:全历史拉链", "41", "进数方式"),
    ZhuiJia("2", "I:追加", "41", "进数方式"),
    ZengLiang("5", "增量拉链", "41", "进数方式"),
    UpSet("7", "F2:UpSert", "41", "进数方式");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    StorageType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "StorageType";

    public static String ofValueByCode(String code) {
        for (StorageType typeCode : StorageType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[StorageType:进数方式]");
    }

    public static StorageType ofEnumByCode(String code) {
        for (StorageType typeCode : StorageType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[StorageType:进数方式]");
    }

    public static String getCodeByValue(String value) {
        for (StorageType typeCode : StorageType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[StorageType:进数方式]");
    }

    public static String ofCatValue() {
        return StorageType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return StorageType.values()[0].getCatCode();
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
