package hyren.serv6.g.enumerate;

import fd.ng.core.utils.StringUtil;

public enum AsynType {

    SYNCHRONIZE("0", "同步返回"), ASYNCALLBACK("1", "异步回调"), ASYNPOLLING("2", "异步轮询");

    private final String code;

    private final String value;

    AsynType(String code, String value) {
        this.code = code;
        this.value = value;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    public static String ofValueByCode(String code) {
        for (AsynType asynType : AsynType.values()) {
            if (asynType.getCode().equals(code)) {
                return asynType.value;
            }
        }
        return null;
    }

    public static AsynType ofEnumByCode(String code) {
        for (AsynType asynType : AsynType.values()) {
            if (asynType.getCode().equals(code)) {
                return asynType;
            }
        }
        return null;
    }

    public static boolean isAsynType(String asynType) {
        if (StringUtil.isNotBlank(asynType) && (AsynType.SYNCHRONIZE == AsynType.ofEnumByCode(asynType) || AsynType.ASYNCALLBACK == AsynType.ofEnumByCode(asynType) || AsynType.ASYNPOLLING == AsynType.ofEnumByCode(asynType))) {
            return true;
        } else {
            return false;
        }
    }
}
