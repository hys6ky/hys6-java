package hyren.serv6.commons.utils.agent;

public interface Increasement {

    void calculateIncrement();

    void mergeIncrement();

    void append();

    void replace();

    void restore(String storageType);

    void close();

    void dropTodayTable();

    void incrementalDataZipper();
}
