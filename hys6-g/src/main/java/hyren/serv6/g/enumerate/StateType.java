package hyren.serv6.g.enumerate;

import hyren.daos.base.utils.ActionResult;

public enum StateType {

    NORMAL(999, "正常"),
    NOT_REST_USER(1001, "该用户非接口用户或者用户不存在"),
    UNAUTHORIZED(1002, "账号或密钥错误"),
    INTERFACE_STATE_ERROR(1003, "接口状态错误,为禁用状态"),
    EFFECTIVE_DATE_ERROR(1004, "接口使用有效期已过期"),
    TABLE_NOT_EXISTENT(1005, "表不存在或者为空"),
    NO_COLUMN_USE_PERMISSIONS(1006, "列没有使用权限"),
    NO_USR_PERMISSIONS(1007, "没有表使用权限"),
    NO_INTERFACE_USE_PERMISSIONS(1008, "没有接口使用权限"),
    START_DATE_ERROR(1009, "接口开始使用日期未到"),
    DIR_ERROR(1010, "存储目录地址错误"),
    FILENAME_ERROR(1011, "文件名称未填写错误"),
    CONDITION_ERROR(1012, "条件填写错误"),
    ARGUMENT_ERROR(1013, "参数错误"),
    UNCUSTOMIZE(1014, "非自定义表"),
    SQL_IS_INCORRECT(1015, "SQL为空或不正确"),
    SQL_UPDATETYPE_ERROR(1016, "SQL更新类型不正确"),
    SQL_DELETETYPE_ERROR(1017, "SQL删除类型不正确"),
    KEY_EXPIRED(1018, "词条填写错误"),
    TOKEN_ERROR(1019, "ToKen填写错误"),
    REPORT_CODE_ERROR(1020, "报表编码错误"),
    JSON_DATA_ERROR(1021, "JSON数据格式不正确"),
    COLUMN_SPECIFICATIONS_ERROR(1022, "字段填写错误"),
    COLUMN_TYPE_ERROR(1023, "字段类型未填写"),
    SOURCE_TABLE_ERROR(1024, "来源表名未填写"),
    TABLE_EXISTENT(1025, "表已经存在"),
    UPLOAD_PICTURE_ERROR(1026, "上传图片错误"),
    CALBACK_URL_ERROR(1027, "响应回调url错误，请检查url是否正确"),
    DATA_TYPE_ERROR(1028, "数据类型dataType错误,未填写或填写错误"),
    OUT_TYPE_ERROR(1029, "输出类型outType错误,未填写或填写错误"),
    INDEX_ERROR(1030, "索引未建立错误"),
    PK_ERROR(1031, "PK信息错误"),
    SIGNAL_FILE_ERROR(1032, "生成信号文件失败，请检查filepath与filename是否正确"),
    ASYNTYPE_ERROR(1033, "异步类型参数asynType错误"),
    FILEPARH_ERROR(1034, "文件路径错误"),
    OPERATION_TYPE_ERROR(1035, "操作类型错误"),
    TOKEN_EXCEPTION(1036, "获取token值失败"),
    IPANDPORT_EXCEPTION(1037, "获取IP与port失败"),
    INTERFACECHECK_EXCEPTION(1038, "接口检查失败"),
    JSONCONVERSION_EXCEPTION(1039, "json转换对象失败"),
    TABLE_DATA_NOT_EXIST_BY_ROWKEY(1040, "根据rowkey要删除的表数据不存在"),
    UUID_NOT_NULL(1041, "uuid不能为空或uuid错误"),
    STORAGELAYER_NOT_EXIST_BY_TABLE(1042, "当前表对应的存储层信息不存在"),
    ORACLE9I_NOT_SUPPORT(1043, "当前表对应的存储层信息不存在"),
    CREATE_FILE_ERROR(1044, "创建文件失败"),
    CREATE_DIRECTOR_ERROR(1045, "创建文件目录失败"),
    URL_NOT_EXIST(1046, "url未填或填写错误"),
    STORE_TYPE_NOT_EXIST(1047, "不支持的存储层类型"),
    DELETE_TABLE_DATA_FAILED(1048, "删除表数据失败"),
    TABLE_NOT_EXIST_ON_HBASE_STOREAGE(1049, "表不在任何hbase存储层中"),
    TABLE_NOT_EXIST_ON_SOLR_STOREAGE(1050, "表不在任何solr存储层中"),
    ONLY_SUPPORT_TABLE_TO_SOLR(1051, "目前只支持查询贴源层以及加工层入solr表"),
    ROWKEY_COLUMN_FORMAT_ERROR(1051, "rowkey查询列名格式有误，格式为：（列族名:列英文名）"),
    COMMUNICATION_ERROR(1052, "当前机器 %s ,端口: %s 通讯异常,无法获取机器资源信息"),
    AGENT_ERROR(1053, "Agent名称: %s ,为获取到相关信息"),
    EXCEPTION(1054, "系统错误");

    private final Integer code;

    private final String message;

    StateType(Integer code, String message) {
        this.code = code;
        this.message = message;
    }

    public Integer getCode() {
        return code;
    }

    public String getMessage() {
        return message;
    }

    public static String ofValueByCode(Integer code) {
        for (StateType stateType : StateType.values()) {
            if (stateType.getCode().equals(code)) {
                return stateType.message;
            }
        }
        return null;
    }

    public static StateType ofEnumByCode(Integer code) {
        for (StateType stateType : StateType.values()) {
            if (stateType.getCode().equals(code)) {
                return stateType;
            }
        }
        return null;
    }

    public static ActionResult getActionResult(StateType stateType) {
        if (stateType.getCode().equals(999)) {
            return ActionResult.success();
        }
        return ActionResult.build(stateType.getCode(), stateType.name(), stateType.getMessage());
    }
}
