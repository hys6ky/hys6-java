package hyren.serv6.base.user;

import hyren.daos.bizpot.commons.ContextDataHolder;

public class UserUtil {

    public static Long getUserId() {
        User user = ContextDataHolder.getUserInfo(User.class);
        return user != null ? user.getUserId() : null;
    }

    public static User getUser() {
        return ContextDataHolder.getUserInfo(User.class);
    }
}
