package hyren.serv6.t.contants;

import hyren.daos.base.exception.SystemBusinessException;

public enum TaskStatusEnum {

    TO_BE_DEV("0", "待开发", "59", "任务状态"),
    DEV_CPLT("1", "开发中", "59", "任务状态"),
    CMT_TEST("2", "提交测试", "59", "任务状态"),
    TESTING("5", "测试中", "59", "任务状态"),
    TO_BE_FB("6", "待反馈", "59", "任务状态"),
    TEST_CPLT("3", "测试通过", "59", "任务状态"),
    TEST_RJT("4", "测试未通过", "59", "任务状态");

    private final String code;

    private final String value;

    private final String catCode;

    private final String catValue;

    TaskStatusEnum(String code, String value, String catCode, String catValue) {
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

    public static final String CodeName = "DevStatusEnum";

    public static String ofValueByCode(String code) {
        for (TaskStatusEnum typeCode : TaskStatusEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode.value;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[DevStatusEnum:任务状态]");
    }

    public static TaskStatusEnum ofEnumByCode(String code) {
        for (TaskStatusEnum typeCode : TaskStatusEnum.values()) {
            if (typeCode.getCode().equals(code)) {
                return typeCode;
            }
        }
        throw new SystemBusinessException("根据[" + code + "]没有找到对应的代码项[DevStatusEnum:任务状态]");
    }

    public static String getCodeByValue(String value) {
        for (TaskStatusEnum typeCode : TaskStatusEnum.values()) {
            if (typeCode.getValue().equals(value)) {
                return typeCode.code;
            }
        }
        throw new SystemBusinessException("根据[" + value + "]没有找到对应的代码项[DevStatusEnum:任务状态]");
    }

    public static String ofCatValue() {
        return TaskStatusEnum.values()[0].getCatValue();
    }

    public static String ofCatCode() {
        return TaskStatusEnum.values()[0].getCatCode();
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
