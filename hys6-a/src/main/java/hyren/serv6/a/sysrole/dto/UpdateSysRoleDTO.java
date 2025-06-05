package hyren.serv6.a.sysrole.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class UpdateSysRoleDTO {

    private long role_id;

    private long[] role_menu;

    private String role_name;

    private String role_remark;

    private String is_admin;
}
