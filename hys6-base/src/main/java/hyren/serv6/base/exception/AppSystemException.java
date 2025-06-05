package hyren.serv6.base.exception;

import hyren.daos.base.exception.SystemRuntimeException;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AppSystemException extends SystemRuntimeException {

    public AppSystemException(final String msg) {
        super(msg);
        log.error(String.format("AppSystemException : %s | %s%s |", getMessage(), LOGCODE_PREFIX, resultCode));
    }

    public AppSystemException(Throwable cause) {
        super(cause);
        log.error(String.format("%s | %s%s |", getMessage(), LOGCODE_PREFIX, resultCode), cause);
    }

    public AppSystemException(String msg, Throwable cause) {
        super(msg, cause);
        log.error(String.format("%s | %s%s |", getMessage(), LOGCODE_PREFIX, resultCode), cause);
    }

    public AppSystemException(int canbeGettedMessage, String loggedMessage, Throwable cause) {
        super(canbeGettedMessage, loggedMessage, cause);
        log.error(String.format("%s | %s%s | %s |", getMessage(), LOGCODE_PREFIX, resultCode, loggedMessage), cause);
    }
}
