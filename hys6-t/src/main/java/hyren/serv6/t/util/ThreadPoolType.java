package hyren.serv6.t.util;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public enum ThreadPoolType {

    FIXED, CACHED, SINGLE, SCHEDULED
}
