package hyren.serv6.stream.agent.producer.commons;

import hyren.serv6.commons.utils.stream.CustomerPartition;
import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import javax.script.Invocable;
import java.util.List;

@Data
public class JobParamsEntity {

    private BusinessProcess businessProcess;

    private Invocable invocable;

    private String cusDesType;

    private String topic;

    private String msgType;

    private String msgHeader;

    private List<String> listColumn;

    private CustomerPartition customerPartition;

    private String bootstrapServers;

    private String jobId;

    private KafkaProducer<String, GenericRecord> producer;

    private KafkaProducer<String, String> producerString;

    private Schema schema;

    private String sdmDateLimiter;

    private String isObj;

    private String sync;

    public BusinessProcess getBusinessProcess() {
        return businessProcess;
    }

    public void setBusinessProcess(BusinessProcess businessProcess) {
        this.businessProcess = businessProcess;
    }

    public Invocable getInvocable() {
        return invocable;
    }

    public void setInvocable(Invocable invocable) {
        this.invocable = invocable;
    }

    public String getCusDesType() {
        return cusDesType;
    }

    public void setCusDesType(String cusDesType) {
        this.cusDesType = cusDesType;
    }

    public String getTopic() {
        return topic;
    }

    public void setTopic(String topic) {
        this.topic = topic;
    }

    public String getMsgType() {
        return msgType;
    }

    public void setMsgType(String msgType) {
        this.msgType = msgType;
    }

    public String getMsgHeader() {
        return msgHeader;
    }

    public void setMsgHeader(String msgHeader) {
        this.msgHeader = msgHeader;
    }

    public List<String> getListColumn() {
        return listColumn;
    }

    public void setListColumn(List<String> listColumn) {
        this.listColumn = listColumn;
    }

    public CustomerPartition getCustomerPartition() {
        return customerPartition;
    }

    public void setCustomerPartition(CustomerPartition customerPartition) {
        this.customerPartition = customerPartition;
    }

    public String getBootstrapServers() {
        return bootstrapServers;
    }

    public void setBootstrapServers(String bootstrapServers) {
        this.bootstrapServers = bootstrapServers;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public KafkaProducer<String, GenericRecord> getProducer() {
        return producer;
    }

    public void setProducer(KafkaProducer<String, GenericRecord> producer) {
        this.producer = producer;
    }

    public KafkaProducer<String, String> getProducerString() {
        return producerString;
    }

    public void setProducerString(KafkaProducer<String, String> producerString) {
        this.producerString = producerString;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public String getSdmDatelimiter() {
        return sdmDateLimiter;
    }

    public void setSdmDatelimiter(String sdmDatelimiter) {
        this.sdmDateLimiter = sdmDatelimiter;
    }

    public String getIsObj() {
        return isObj;
    }

    public void setIsObj(String isObj) {
        this.isObj = isObj;
    }

    public String getSync() {
        return sync;
    }

    public void setSync(String sync) {
        this.sync = sync;
    }
}
