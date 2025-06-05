package hyren.serv6.r.util;

public class TempTableConf {

    public static final String OPERATION_COLUMN_NAME = "HYREN_OPERATION";

    public static final String OPERATION_COLUMN_TYPE = "VARCHAR(1)";

    public static enum TempTableOperation {

        update("1", "修改"), insert("2", "新增");

        private final String code;

        private final String value;

        private TempTableOperation(String code, String value) {
            this.code = code;
            this.value = value;
        }

        public String getCode() {
            return this.code;
        }

        public String getValue() {
            return this.value;
        }
    }
}
