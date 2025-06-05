package hyren.serv6.b.realtimecollection.bean;

import lombok.Data;
import org.springframework.stereotype.Component;

@Data
@Component
public class BrokersDomain {

    private int id = 0;

    private String host = "";

    private int port = 0;

    private String created = "";

    private String modify = "";
}
