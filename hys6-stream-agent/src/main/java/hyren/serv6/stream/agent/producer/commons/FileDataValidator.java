package hyren.serv6.stream.agent.producer.commons;

public interface FileDataValidator {

    boolean isNewLine(String lineText);

    boolean isSkipLine(String lineText);
}
