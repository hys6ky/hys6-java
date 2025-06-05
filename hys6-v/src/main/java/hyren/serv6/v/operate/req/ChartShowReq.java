package hyren.serv6.v.operate.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ChartShowReq {

    private String exe_sql;

    private String[] x_columns;

    private String[] y_columns;

    private String chart_type;

    private Integer showNum;
}
