package hyren.serv6.commons.cache;

import fd.ng.core.annotation.DocClass;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@DocClass(desc = "", author = "博彦科技", createdate = "2020/8/17 0017 下午 02:29")
public class ConcurrentHashMapCacheUtil {

    private CacheConfBean confBean;

    public Integer current_size = 0;

    public Map<String, CacheObj> cache_object_map = new ConcurrentHashMap<>();

    public List<String> cache_use_log_list = new LinkedList<>();

    public Boolean clean_thread_is_run = false;

    public ConcurrentHashMapCacheUtil(CacheConfBean cacheConfBean) {
        confBean = cacheConfBean;
        ExecutorService executor = Executors.newSingleThreadExecutor();
        if (!clean_thread_is_run) {
            executor.submit(new CleanTimeOutThread());
        }
    }

    public void setCache(String cacheKey, Object cacheValue, long cacheTime) {
        Long ttlTime = null;
        if (cacheTime <= 0L) {
            ttlTime = cacheTime == -1L ? -1L : null;
        }
        checkSize();
        saveCacheUseLog(cacheKey);
        current_size = current_size + 1;
        if (ttlTime == null) {
            ttlTime = System.currentTimeMillis() + cacheTime;
        }
        CacheObj cacheObj = new CacheObj(cacheKey, cacheValue, ttlTime);
        cache_object_map.put(cacheKey, cacheObj);
    }

    public void setCache(String cacheKey, Object cacheValue) {
        setCache(cacheKey, cacheValue, confBean.getCache_time());
    }

    public CacheObj getCache(String cacheKey) {
        CacheObj cacheObj = null;
        if (checkCache(cacheKey)) {
            saveCacheUseLog(cacheKey);
            cacheObj = cache_object_map.get(cacheKey);
        }
        return cacheObj;
    }

    public boolean isExist(String cacheKey) {
        return checkCache(cacheKey);
    }

    public void clear() {
        cache_object_map.clear();
        current_size = 0;
    }

    public void deleteCache(String cacheKey) {
        Object cacheValue = cache_object_map.remove(cacheKey);
        if (cacheValue != null) {
            current_size = current_size - 1;
        }
    }

    private boolean checkCache(String cacheKey) {
        CacheObj cacheObj = cache_object_map.get(cacheKey);
        if (cacheObj == null) {
            return false;
        }
        if (cacheObj.getTtlTime() == -1L) {
            return true;
        }
        if (cacheObj.getTtlTime() < System.currentTimeMillis()) {
            deleteCache(cacheKey);
            return false;
        }
        return true;
    }

    private void deleteLRU() {
        String cacheKey = cache_use_log_list.remove(cache_use_log_list.size() - 1);
        deleteCache(cacheKey);
    }

    public void deleteTimeOut() {
        for (Map.Entry<String, CacheObj> entry : cache_object_map.entrySet()) {
            if (entry.getValue().getTtlTime() < System.currentTimeMillis() && entry.getValue().getTtlTime() != -1L) {
                deleteCache(entry.getKey());
            }
        }
    }

    private void checkSize() {
        if (current_size >= confBean.getCache_max_number()) {
            deleteTimeOut();
        }
        if (current_size >= confBean.getCache_max_number()) {
            deleteLRU();
        }
    }

    private synchronized void saveCacheUseLog(String cacheKey) {
        cache_use_log_list.remove(cacheKey);
        cache_use_log_list.add(0, cacheKey);
    }

    public void setCleanThreadRun(Boolean boo) {
        clean_thread_is_run = boo;
    }

    private void startCleanThread() {
    }

    class CleanTimeOutThread implements Runnable {

        @Override
        public void run() {
            setCleanThreadRun(Boolean.TRUE);
            while (true) {
                try {
                    deleteTimeOut();
                    Thread.sleep(confBean.getCache_cleaning_frequency());
                } catch (InterruptedException e) {
                    e.printStackTrace();
                    setCleanThreadRun(Boolean.FALSE);
                    System.exit(-1);
                }
            }
        }
    }

    public void showUtilsInfo() {
        System.out.println("cache clean thread is run :" + clean_thread_is_run);
        System.out.println("cache max count is :" + confBean.getCache_max_number());
        System.out.println("cache current count is :" + current_size);
        System.out.println("cache object map is :" + cache_object_map.toString());
        System.out.println("cache use log list is :" + cache_use_log_list.toString());
    }

    public static void main(String[] args) {
        CacheConfBean cacheConfBean = new CacheConfBean();
        cacheConfBean.setCache_time(2 * 5 * 1000L);
        cacheConfBean.setCache_max_number(1000);
        cacheConfBean.setCache_cleaning_frequency(2 * 5 * 1000L);
        ConcurrentHashMapCacheUtil concurrentHashMapCacheUtil = new ConcurrentHashMapCacheUtil(cacheConfBean);
        concurrentHashMapCacheUtil.setCache("my_cache_key", "my_cache_key_value");
        concurrentHashMapCacheUtil.showUtilsInfo();
        while (true) {
            try {
                Thread.sleep(1000L);
                concurrentHashMapCacheUtil.showUtilsInfo();
                if (concurrentHashMapCacheUtil.current_size == 0) {
                    System.out.println("aaa" + concurrentHashMapCacheUtil.current_size);
                    concurrentHashMapCacheUtil.setCache("my_cache_key_aaa", "aaa_test", 10 * 1000);
                    System.out.println("bbb" + concurrentHashMapCacheUtil.current_size);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
                concurrentHashMapCacheUtil.setCleanThreadRun(Boolean.FALSE);
                System.exit(-1);
            }
        }
    }
}
