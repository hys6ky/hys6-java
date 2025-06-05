package hyren.serv6.commons.cache;

public class CacheConfBean {

    private Long cache_time = 10 * 60 * 1000L;

    private Integer cache_max_number = 1000;

    private Long cache_cleaning_frequency = 10 * 60 * 1000L;

    public Long getCache_time() {
        return cache_time;
    }

    public void setCache_time(Long cache_time) {
        this.cache_time = cache_time;
    }

    public Integer getCache_max_number() {
        return cache_max_number;
    }

    public void setCache_max_number(Integer cache_max_number) {
        this.cache_max_number = cache_max_number;
    }

    public Long getCache_cleaning_frequency() {
        return cache_cleaning_frequency;
    }

    public void setCache_cleaning_frequency(Long cache_cleaning_frequency) {
        this.cache_cleaning_frequency = cache_cleaning_frequency;
    }
}
