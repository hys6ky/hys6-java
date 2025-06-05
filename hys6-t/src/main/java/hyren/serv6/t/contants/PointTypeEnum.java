package hyren.serv6.t.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum PointTypeEnum {

    BIZ("0", "表", "59", "测试要点"), DATA("1", "数据需求", "59", "测试要点");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    PointTypeEnum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "PointTypeEnum";

    public static String ofValueByCode(String code) {
        for (PointTypeEnum typeCode : PointTypeEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[PointTypeEnum:测试要点]");
    }

    public static PointTypeEnum ofEnumByCode(String code) {
        for (PointTypeEnum typeCode : PointTypeEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[PointTypeEnum:测试要点]");
    }

    public static String getCodeByValue(String value) {
        for (PointTypeEnum typeCode : PointTypeEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[PointTypeEnum:测试要点]");
    }

    public static String ofCatValue() {
        return PointTypeEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return PointTypeEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
