package hyren.serv6.k.dbm.codetypeinfo.bean;

import hyren.serv6.k.entity.DbmCodeItemInfo;
import hyren.serv6.k.entity.DbmCodeTypeInfo;
import lombok.Data;
import java.util.List;

@Data
public class CodeTypeAndItemInfoDto extends DbmCodeTypeInfo {

    private List<DbmCodeItemInfo> itemInfos;
}
