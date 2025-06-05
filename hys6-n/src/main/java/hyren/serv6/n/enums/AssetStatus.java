package hyren.serv6.n.enums;

import hyren.daos.base.exception.SystemBusinessException;

public enum AssetStatus {

    DAIPANDIAN("1", "待盘点", "", "资产状态"), LINSHIBAOCUN("2", "临时保存", "", "资产状态"), YIWANCHENGDENGJI("3", "已完成登记", "", "资产状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    AssetStatus(String code, String value, String catCode, String catValue) {
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
        for (AssetStatus typeCode : AssetStatus.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[AssetType:资产状态]");
    }

    public static AssetStatus ofEnumByCode(String code) {
        for (AssetStatus typeCode : AssetStatus.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[AssetType:资产状态]");
    }

    public static String getCodeByValue(String value) {
        for (AssetStatus typeCode : AssetStatus.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[AssetType:资产状态]");
    }

    public static String ofCatValue() {
        return AssetStatus.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return AssetStatus.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
