package hyren.serv6.t.vo.query;

import hyren.serv6.t.entity.TskTaskDev;
import lombok.Data;

@Data
public class TskTaskDevQueryVo extends TskTaskDev {

    private String data_req_name;

    private String biz_name;
}
