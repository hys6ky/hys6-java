package hyren.serv6.h.process_flink.bean;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlinkJDBCConnectParams {

    private String url;

    private String tableName;

    private String username;

    private String pwd;
}
