package hyren.serv6.gateway;

import hyren.daos.gateauth.handler.AuthorityMatcher;
import org.springframework.stereotype.Component;
import java.util.List;

@Component
public class MyAuthorityMatcher implements AuthorityMatcher {

    public boolean onWork(List<String> userResourceList, String uri) {
        for (String sourcePath : userResourceList) {
            if (uri.startsWith(sourcePath + '/'))
                return true;
        }
        return false;
    }
}
