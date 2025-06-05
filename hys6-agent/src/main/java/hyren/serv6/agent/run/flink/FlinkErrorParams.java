package hyren.serv6.agent.run.flink;

public enum FlinkErrorParams {

    FLINK_CDC_CONSUMER_STARTED(100, "[flink-cdc] consumer-started"),
    FLINK_CDC_CONSUMER_NO_JDBC(110, "[flink-cdc] consumer-no-jdbc"),
    FLINK_CDC_CONSUMER_MAIN_PARAMS_ERROR(120, "[flink-cdc] main-params-error"),
    FLINK_CDC_CONSUMER_ERROR(130, "[flink-cdc] consumer-error"),
    FLINK_CDC_JDBC_ERROR(140, "[flink-cdc] jdbc-error"),
    FLINK_CDC_CONSUMER_RUN_TIMEOUT(150, "[flink-cdc] run-timeout"),
    FLINK_CDC_CONSUMER_GET_PARAMS_FAIL(160, "[flink-cdc] get-params-fail"),
    FLINK_CDC_PRODUCER_STARTED(200, "[flink-cdc] producer-started"),
    FLINK_CDC_PRODUCER_ERROR(210, "[flink-cdc] producer-error"),
    FLINK_CDC_PRODUCER_RUN_TIMEOUT(220, "[flink-cdc] run-timeout"),
    FLINK_CDC_PRODUCER_GET_PARAMS_FAIL(230, "[flink-cdc] get-params-fail"),
    FLINK_CDC_PRODUCER_CHECKPOINT_URL_IS_ERROR(240, "[flink-cdc] checkpoint-url-iserror"),
    FLINK_CDC_PRODUCER_MAIN_PARAMS_ERROR(250, "[flink-cdc] main-params-error");

    FlinkErrorParams(int code, String value) {
        this.code = code;
        this.value = value;
    }

    private final int code;

    private final String value;

    public int getCode() {
        return code;
    }

    public String getValue() {
        return value;
    }
}
