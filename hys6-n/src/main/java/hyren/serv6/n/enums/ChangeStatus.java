package hyren.serv6.n.enums;

import hyren.daos.base.exception.SystemBusinessException;

public enum ChangeStatus {

    WEIBIANGENG("1", "未变更", "", "变更状态"), DAIGENGXIN("2", "待更新", "", "变更状态"), YIGENGXIN("3", "已更新", "", "变更状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    ChangeStatus(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "ChangeStatus";

    public static String ofValueByCode(String code) {
        for (ChangeStatus typeCode : ChangeStatus.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ChangeStatus:变更状态]");
    }

    public static ChangeStatus ofEnumByCode(String code) {
        for (ChangeStatus typeCode : ChangeStatus.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[ChangeStatus:变更状态]");
    }

    public static String getCodeByValue(String value) {
        for (ChangeStatus typeCode : ChangeStatus.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[ChangeStatus:变更状态]");
    }

    public static String ofCatValue() {
        return ChangeStatus.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return ChangeStatus.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
