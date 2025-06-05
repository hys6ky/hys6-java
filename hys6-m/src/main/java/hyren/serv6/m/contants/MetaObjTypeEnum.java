package hyren.serv6.m.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum MetaObjTypeEnum {

    TBL("0", "表", "59", "元数据对象类型"), VIEW("1", "视图", "59", "元数据对象类型"), PROC("2", "存储过程", "59", "元数据对象类型"), METER_VIEW("3", "物化视图", "59", "元数据对象类型");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    MetaObjTypeEnum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "MetaObjTypeEnum";

    public static String ofValueByCode(String code) {
        for (MetaObjTypeEnum typeCode : MetaObjTypeEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[MetaObjTypeEnum:元数据对象类型]");
    }

    public static MetaObjTypeEnum ofEnumByCode(String code) {
        for (MetaObjTypeEnum typeCode : MetaObjTypeEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[MetaObjTypeEnum:元数据对象类型]");
    }

    public static String getCodeByValue(String value) {
        for (MetaObjTypeEnum typeCode : MetaObjTypeEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[MetaObjTypeEnum:元数据对象类型]");
    }

    public static String ofCatValue() {
        return MetaObjTypeEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return MetaObjTypeEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
