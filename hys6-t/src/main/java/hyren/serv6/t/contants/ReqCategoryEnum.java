package hyren.serv6.t.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum ReqCategoryEnum {

    BIZ("0", "业务需求", "59", "需求类别"), DATA("1", "数据需求", "59", "需求类别");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ReqCategoryEnum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ReqCategoryEnum";

    public static String ofValueByCode(String code) {
        for (ReqCategoryEnum typeCode : ReqCategoryEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ReqCategoryEnum:需求类别]");
    }

    public static ReqCategoryEnum ofEnumByCode(String code) {
        for (ReqCategoryEnum typeCode : ReqCategoryEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ReqCategoryEnum:需求类别]");
    }

    public static String getCodeByValue(String value) {
        for (ReqCategoryEnum typeCode : ReqCategoryEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[ReqCategoryEnum:需求类别]");
    }

    public static String ofCatValue() {
        return ReqCategoryEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ReqCategoryEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
