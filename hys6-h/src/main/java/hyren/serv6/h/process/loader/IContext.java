package hyren.serv6.h.process.loader;

public interface IContext {

    void startJob();

    void endJob(boolean isSuccessful);
}
