package hyren.serv6.base.codes;

import hyren.daos.base.exception.SystemRuntimeException;
import java.io.ByteArrayOutputStream;
import java.io.ObjectOutputStream;
import java.util.Base64;

public enum StoreLayerAdded {

    ZhuJian("01", "主键", "62", "存储层附件属性"),
    RowKey("02", "rowkey", "62", "存储层附件属性"),
    SuoYinLie("03", "索引列", "62", "存储层附件属性"),
    YuJuHe("04", "预聚合列", "62", "存储层附件属性"),
    PaiXuLie("05", "排序列", "62", "存储层附件属性"),
    FenQuLie("06", "分区列", "62", "存储层附件属性"),
    Solr("07", "Solr列", "62", "存储层附件属性");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    StoreLayerAdded(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "StoreLayerAdded";

    public static String ofValueByCode(String code) {
        for (StoreLayerAdded typeCode : StoreLayerAdded.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[StoreLayerAdded:存储层附件属性]");
    }

    public static StoreLayerAdded ofEnumByCode(String code) {
        for (StoreLayerAdded typeCode : StoreLayerAdded.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemRuntimeException("根据[" + code + "]没有找到对应的代码项[StoreLayerAdded:存储层附件属性]");
    }

    public static String getCodeByValue(String value) {
        for (StoreLayerAdded typeCode : StoreLayerAdded.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemRuntimeException("根据[" + value + "]没有找到对应的代码项[StoreLayerAdded:存储层附件属性]");
    }

    public static String ofCatValue() {
        return StoreLayerAdded.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return StoreLayerAdded.values()[0].getCatCode();
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
