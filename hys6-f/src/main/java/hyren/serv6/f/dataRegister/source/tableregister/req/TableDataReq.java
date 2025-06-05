package hyren.serv6.f.dataRegister.source.tableregister.req;

import hyren.serv6.base.entity.TableInfo;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TableDataReq extends TableInfo {

    private static final long serialVersionUID = 7176661710290391778L;

    private String is_prefix;
}
