package hyren.serv6.a.node;

import io.swagger.annotations.ApiModel;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;

@Data
@ApiModel(value = "", description = "")
@AllArgsConstructor
@NoArgsConstructor
public class TreeNode {

    private Long id;

    private String label;

    private String enmm;

    private Long pId;

    private List<TreeNode> children;

    private String type;

    public void initChildren() {
        this.children = new ArrayList<>();
    }
}
