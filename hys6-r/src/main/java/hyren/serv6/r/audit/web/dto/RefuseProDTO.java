package hyren.serv6.r.audit.web.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class RefuseProDTO {

    private List<Long> df_pids;

    private String audit_opinion;

    private String audit_remarks;
}
