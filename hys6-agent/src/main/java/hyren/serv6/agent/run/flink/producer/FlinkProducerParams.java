package hyren.serv6.agent.run.flink.producer;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.postgresql.shaded.com.ongres.scram.common.bouncycastle.pbkdf2.RuntimeCryptoException;
import fd.ng.core.utils.StringUtil;
import hyren.serv6.commons.utils.constant.DataBaseType;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class FlinkProducerParams implements Serializable {

    private static final long serialVersionUID = -8809742894711583471L;

    private String checkpoint_uri;

    private String lastJobId;

    private DataBaseType database_type;

    private String database_ip;

    private int database_port;

    private String database_username;

    private String database_password;

    private String database_name;

    private String database_schema;

    private List<FlinkCDCTable> tables;

    public void setDataBaseType(String type) {
        this.database_type = DataBaseType.valueOf(type);
    }

    public void verify() throws RuntimeException {
        if (StringUtil.isEmpty(checkpoint_uri)) {
            throw new RuntimeCryptoException("检查点地址不可为空");
        }
        if (database_type == null) {
            throw new RuntimeException("数据库类型不可为空");
        }
        if (StringUtil.isEmpty(database_ip)) {
            throw new RuntimeException("主机名不可为空");
        }
        if (database_port <= 0 || database_port > 65535) {
            throw new RuntimeException("端口号不可为空");
        }
        if (StringUtil.isEmpty(database_name)) {
            throw new RuntimeException("数据库不可为空");
        }
        if (StringUtil.isEmpty(database_username)) {
            throw new RuntimeException("用户名不可为空");
        }
        if (database_password == null) {
            throw new RuntimeException("密码不可为空");
        }
        if (tables == null || tables.size() <= 0) {
            throw new RuntimeException("采集列表为空");
        } else {
            for (FlinkCDCTable table : tables) {
                table.verify();
            }
        }
    }
}
