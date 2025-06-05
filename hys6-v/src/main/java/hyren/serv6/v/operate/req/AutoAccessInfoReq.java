package hyren.serv6.v.operate.req;

import hyren.serv6.base.entity.AutoFetchRes;
import hyren.serv6.base.entity.AutoFetchSum;
import hyren.serv6.base.entity.AutoTpCondInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class AutoAccessInfoReq {

    AutoFetchSum auto_fetch_sum;

    AutoTpCondInfo[] autoTpCondInfos;

    AutoFetchRes[] autoFetchRes;
}
