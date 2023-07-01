package org.example;

import Utils.kafkaUtils;
import com.thoughtworks.gauge.Step;

public class kafkaPublishingMessageImpl {

    kafkaUtils ku = new kafkaUtils();

    @Step("create kafkaApp connection with cloudkarafka")
    public void createConnectionsStep() {
        ku.createKafkaConnection();
        }

    @Step("Send to topic <topic> with message <message>")
    public void sendMessageStep(String topic,String message) {
        ku.sendMessageToKafka(topic,message);
    }

    @Step("Consume message from topic <topic>")
    public void consumeMessageFromTopics(String topic) {
        ku.consumeMessageFromTopics(topic);
    }
}
