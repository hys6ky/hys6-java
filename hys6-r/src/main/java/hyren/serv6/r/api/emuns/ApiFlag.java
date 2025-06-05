package hyren.serv6.r.api.emuns;

import hyren.daos.base.exception.SystemBusinessException;

public enum ApiFlag {

    SAVE("1", "修改"), UPDATE("2", "新增");

    private final String code;

    private final String value;

    ApiFlag(String code, String value) {
        this.code = code;
        this.value = value;
    }

    public String getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        throw new SystemBusinessException("There‘s no need for you to !");
    }
}
