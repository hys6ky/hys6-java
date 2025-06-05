package hyren.serv6.t.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum ReqStatusEnum {

    TO_BE_DEV("0", "待开发", "59", "需求状态"), DEV_ING("1", "开发中", "59", "需求状态"), FINISH("3", "已结项", "59", "需求状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ReqStatusEnum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ReqStatusEnum";

    public static String ofValueByCode(String code) {
        for (ReqStatusEnum typeCode : ReqStatusEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ReqStatusEnum:需求状态]");
    }

    public static ReqStatusEnum ofEnumByCode(String code) {
        for (ReqStatusEnum typeCode : ReqStatusEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ReqStatusEnum:需求状态]");
    }

    public static String getCodeByValue(String value) {
        for (ReqStatusEnum typeCode : ReqStatusEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[ReqStatusEnum:需求状态]");
    }

    public static String ofCatValue() {
        return ReqStatusEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ReqStatusEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
