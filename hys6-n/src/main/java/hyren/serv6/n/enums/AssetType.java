package hyren.serv6.n.enums;

import hyren.daos.base.exception.SystemBusinessException;

public enum AssetType {

    BIAO("1", "表", "", "资产类型"), SHITU("2", "视图", "", "资产类型"), WENJIAN("3", "文件", "", "资产类型"), ZHIBIAO("4", "指标", "", "资产类型"), ZIDUAN("5", "字段", "", "资产类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AssetType(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "AssetType";

    public static String ofValueByCode(String code) {
        for (AssetType typeCode : AssetType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[AssetType:资产类型]");
    }

    public static AssetType ofEnumByCode(String code) {
        for (AssetType typeCode : AssetType.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[AssetType:资产类型]");
    }

    public static String getCodeByValue(String value) {
        for (AssetType typeCode : AssetType.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[AssetType:资产类型]");
    }

    public static String ofCatValue() {
        return AssetType.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AssetType.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
