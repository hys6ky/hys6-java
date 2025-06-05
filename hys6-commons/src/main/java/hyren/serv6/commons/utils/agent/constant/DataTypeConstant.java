package hyren.serv6.commons.utils.agent.constant;

import hyren.serv6.commons.enumtools.EnumConstantInterface;

public enum DataTypeConstant implements EnumConstantInterface {

    STRING(0, "string"),
    CHAR(1, "char"),
    BOOLEAN(3, "boolean"),
    INT(4, "int"),
    DECIMAL(5, "decimal"),
    FLOAT(6, "float"),
    DOUBLE(7, "double"),
    INT8(8, "int8"),
    BIGINT(9, "bigint"),
    LONG(10, "long"),
    NUMERIC(11, "numeric"),
    BYTE(12, "byte"),
    TIMESTAMP(13, "timestamp"),
    DATE(14, "date"),
    CLOB(15, "clob"),
    BLOB(16, "blob"),
    NUMBER(17, "number"),
    DATETIME(18, "datetime");

    private final int code;

    private final String message;

    DataTypeConstant(int code, String message) {
        this.code = code;
        this.message = message;
    }

    @Override
    public int getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return message;
    }
}
