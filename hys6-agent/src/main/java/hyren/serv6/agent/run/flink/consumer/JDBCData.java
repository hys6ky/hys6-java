package hyren.serv6.agent.run.flink.consumer;

import java.util.List;
import fd.ng.db.jdbc.DatabaseWrapper;
import hyren.serv6.commons.collection.bean.JDBCBean;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class JDBCData extends JDBCBean {

    private String before_table_name;

    private String after_table_name;

    private List<Column> columns;

    private boolean isExist;

    private DatabaseWrapper db;
}
