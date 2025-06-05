package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum DbmDataType {

    BianMaLei("101", "编码类", "67", "对标数据类别"),
    BiaoShiLei("102", "标识类", "67", "对标数据类别"),
    DaiMaLei("103", "代码类", "67", "对标数据类别"),
    JinELei("104", "金额类", "67", "对标数据类别"),
    RiQiLei("105", "日期类", "67", "对标数据类别"),
    RiQiShiJianLei("106", "日期时间类", "67", "对标数据类别"),
    ShiJianLei("107", "时间类", "67", "对标数据类别"),
    ShuZhiLei("108", "数值类", "67", "对标数据类别"),
    WenBenLei("109", "文本类", "67", "对标数据类别");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    DbmDataType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "DbmDataType";

    public static String ofValueByCode(String code) {
        for (DbmDataType typeCode : DbmDataType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DbmDataType:对标数据类别]");
    }

    public static DbmDataType ofEnumByCode(String code) {
        for (DbmDataType typeCode : DbmDataType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[DbmDataType:对标数据类别]");
    }

    public static String getCodeByValue(String value) {
        for (DbmDataType typeCode : DbmDataType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[DbmDataType:对标数据类别]");
    }

    public static String ofCatValue() {
        return DbmDataType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return DbmDataType.values()[0].getCatCode();
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
