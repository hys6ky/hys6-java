package hyren.serv6.b.batchcollection.cdcCollect.req;

import java.util.List;
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
}
