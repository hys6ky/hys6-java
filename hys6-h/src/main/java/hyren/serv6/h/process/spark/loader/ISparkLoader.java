package hyren.serv6.h.process.spark.loader;

import java.io.Serializable;

public interface ISparkLoader extends Serializable {

    void append();

    void replace();

    void upSert();

    void historyZipperFullLoading();

    void historyZipperIncrementLoading();

    void incrementalDataZipper();
}
