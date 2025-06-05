package hyren.serv6.b.datareceive.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class BatchAnals {

    private Long dr_task_id;

    private String drParams;

    private String curr_bath_date;
}
