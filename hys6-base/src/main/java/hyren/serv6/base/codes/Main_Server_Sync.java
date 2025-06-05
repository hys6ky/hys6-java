package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum Main_Server_Sync {

    LOCK("L", "锁定", "27", "ETL主服务器同步"), NO("N", "不同步", "27", "ETL主服务器同步"), YES("Y", "同步", "27", "ETL主服务器同步"), BACKUP("B", "备份中", "27", "ETL主服务器同步");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    Main_Server_Sync(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "Main_Server_Sync";

    public static String ofValueByCode(String code) {
        for (Main_Server_Sync typeCode : Main_Server_Sync.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Main_Server_Sync:ETL主服务器同步]");
    }

    public static Main_Server_Sync ofEnumByCode(String code) {
        for (Main_Server_Sync typeCode : Main_Server_Sync.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[Main_Server_Sync:ETL主服务器同步]");
    }

    public static String getCodeByValue(String value) {
        for (Main_Server_Sync typeCode : Main_Server_Sync.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[Main_Server_Sync:ETL主服务器同步]");
    }

    public static String ofCatValue() {
        return Main_Server_Sync.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return Main_Server_Sync.values()[0].getCatCode();
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
