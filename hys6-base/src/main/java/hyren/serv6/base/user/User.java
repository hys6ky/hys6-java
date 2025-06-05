package hyren.serv6.base.user;

import hyren.daos.base.web.UserInfoDefaultImpl;
import lombok.*;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@EqualsAndHashCode(callSuper = true)
public class User extends UserInfoDefaultImpl implements Serializable {

    private static final long serialVersionUID = -6710574617261895782L;

    private Long userId;

    private Long roleId;

    private String userEmail;

    private String userMobile;

    private String userType;

    private String loginIp;

    private String loginDate;

    private String userState;

    private Long depId;

    private String depName;

    private String roleName;

    private String userTypeGroup;

    private String is_login;

    private String limitMultiLogin;

    private String nickname;
}
