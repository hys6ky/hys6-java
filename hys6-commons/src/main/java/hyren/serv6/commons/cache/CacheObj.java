package hyren.serv6.commons.cache;

public class CacheObj {

    private Object CacheKey;

    private Object CacheValue;

    private Long ttlTime;

    public Object getCacheKey() {
        return CacheKey;
    }

    public void setCacheKey(Object cacheKey) {
        CacheKey = cacheKey;
    }

    public Object getCacheValue() {
        return CacheValue;
    }

    public void setCacheValue(Object cacheValue) {
        CacheValue = cacheValue;
    }

    public Long getTtlTime() {
        return ttlTime;
    }

    public void setTtlTime(Long ttlTime) {
        this.ttlTime = ttlTime;
    }

    CacheObj(String cacheKey, Object cacheValue, Long ttl_time) {
        CacheKey = cacheKey;
        CacheValue = cacheValue;
        ttlTime = ttl_time;
    }

    @Override
    public String toString() {
        return "CacheObj{CacheValue=" + CacheValue + ", ttlTime=" + ttlTime + '}';
    }
}
