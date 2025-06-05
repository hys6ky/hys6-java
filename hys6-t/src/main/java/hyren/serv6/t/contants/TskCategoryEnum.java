package hyren.serv6.t.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum TskCategoryEnum {

    DATA_PROCESS("3", "数据加工", "59", "任务类别"), META_DATA("4", "元数据配置", "59", "任务类别"), DATA_ASSET("5", "数据资产盘点", "59", "任务类别");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    TskCategoryEnum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "TskCategoryEnum";

    public static String ofValueByCode(String code) {
        for (TskCategoryEnum typeCode : TskCategoryEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[TskCategoryEnum:任务类别]");
    }

    public static TskCategoryEnum ofEnumByCode(String code) {
        for (TskCategoryEnum typeCode : TskCategoryEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[TskCategoryEnum:任务类别]");
    }

    public static String getCodeByValue(String value) {
        for (TskCategoryEnum typeCode : TskCategoryEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[TskCategoryEnum:任务类别]");
    }

    public static String ofCatValue() {
        return TskCategoryEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return TskCategoryEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
