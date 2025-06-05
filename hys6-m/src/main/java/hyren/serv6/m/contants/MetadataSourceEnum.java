package hyren.serv6.m.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum MetadataSourceEnum {

    DB("0", "数据库直连", "59", "元数据来源"), IMPORT("1", "外部数据导入", "59", "元数据来源");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    MetadataSourceEnum(String code, String value, String catCode, String catValue) {
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
        for (MetadataSourceEnum typeCode : MetadataSourceEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[MetaObjTypeEnum:元数据来源]");
    }

    public static MetadataSourceEnum ofEnumByCode(String code) {
        for (MetadataSourceEnum typeCode : MetadataSourceEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[MetaObjTypeEnum:元数据来源]");
    }

    public static String getCodeByValue(String value) {
        for (MetadataSourceEnum typeCode : MetadataSourceEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[MetaObjTypeEnum:元数据来源]");
    }

    public static String ofCatValue() {
        return MetadataSourceEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return MetadataSourceEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
