package hyren.serv6.n.enums;

import hyren.daos.base.exception.SystemBusinessException;

public enum PublishStatus {

    YIFABU("1", "已发布", "", "发布状态"), WEIFABU("2", "未发布", "", "发布状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    PublishStatus(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "PublishStatus";

    public static String ofValueByCode(String code) {
        for (PublishStatus typeCode : PublishStatus.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[PublishStatus:发布状态]");
    }

    public static PublishStatus ofEnumByCode(String code) {
        for (PublishStatus typeCode : PublishStatus.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[PublishStatus:发布状态]");
    }

    public static String getCodeByValue(String value) {
        for (PublishStatus typeCode : PublishStatus.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[PublishStatus:发布状态]");
    }

    public static String ofCatValue() {
        return PublishStatus.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return PublishStatus.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
