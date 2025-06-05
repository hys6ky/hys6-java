package hyren.serv6.base.exception;

import hyren.daos.base.exception.SystemBusinessException;
import hyren.daos.bizpot.commons.ContextDataHolder;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class BusinessException extends SystemBusinessException {

    private static final long serialVersionUID = 8714959098642865641L;

    public BusinessException(final String message) {
        super(message);
        log.info("{} BusinessException : {}", ContextDataHolder.getBizId(), getMessage());
    }

    public BusinessException(final String resKeyName, final Object[] resArgs) {
        super(resKeyName, resArgs);
        log.info("{} BusinessException : {}", ContextDataHolder.getBizId(), getMessage());
    }

    public BusinessException(final int code, final String message) {
        super(code, message);
        log.info("{} BusinessException : {} --> {}", ContextDataHolder.getBizId(), code, getMessage());
    }

    public BusinessException(final int code, final String resKeyName, final Object[] resArgs) {
        super(code, resKeyName, resArgs);
        log.info("{} BusinessException : {} --> {}", ContextDataHolder.getBizId(), code, getMessage());
    }

    public BusinessException(final ExceptionMessage exMsg) {
        this(exMsg.getCode(), exMsg.getMessage());
    }
}
