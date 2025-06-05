package hyren.serv6.b.datareceive.req;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TreeNode {

    private Long id;

    private Long pId;

    private String label;
}
