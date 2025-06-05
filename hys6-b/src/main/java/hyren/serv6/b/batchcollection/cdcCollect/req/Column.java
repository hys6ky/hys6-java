package hyren.serv6.b.batchcollection.cdcCollect.req;

import hyren.serv6.base.codes.IsFlag;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Column {

    private String column_name;

    private String column_type;

    private String column_tar_type;

    private IsFlag is_primary_key = IsFlag.Fou;
}
