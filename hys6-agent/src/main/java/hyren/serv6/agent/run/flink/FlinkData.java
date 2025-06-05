package hyren.serv6.agent.run.flink;

import java.util.Map;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.constant.DataBaseType;
import lombok.Data;

@Data
public class FlinkData {

    private Map<String, Object> before;

    private Map<String, Object> after;

    private Source source;

    private String op;

    private Long ts_ms;

    private Object transaction;

    public Operation getOperation() {
        if (this.op != null) {
            switch(this.op) {
                case "c":
                    return Operation.create;
                case "u":
                    return Operation.update;
                case "d":
                    return Operation.delete;
                case "r":
                    return Operation.reade;
                default:
                    break;
            }
        }
        return null;
    }

    public DataBaseType getDataBaseType() {
        return this.source.getDataBaseType();
    }

    public void verify() {
        if (before == null && after == null) {
            throw new NullPointerException("before 和 after 不可同时为 null");
        }
        if (StringUtil.isEmpty(op)) {
            throw new NullPointerException("操作类型 op 不可为空");
        }
        if (source == null) {
            throw new NullPointerException("数据源 source 不可为空");
        }
        if (source.connector == null) {
            throw new NullPointerException("数据源类型 connector 不可为空");
        }
    }

    public static enum Operation {

        create, update, delete, reade
    }

    @Data
    public static class Source {

        private String version;

        private String connector;

        private String name;

        private Long ts_ms;

        private String snapshot;

        private String db;

        private Object sequence;

        private String table;

        private Long server_id;

        private Object gtid;

        private String file;

        private Long pos;

        private Long row;

        private Object thread;

        private Object query;

        public DataBaseType getDataBaseType() {
            if (this.connector != null) {
                if (this.connector.toLowerCase().equals("mysql")) {
                    return DataBaseType.MYSQL;
                }
            }
            return null;
        }
    }
}
