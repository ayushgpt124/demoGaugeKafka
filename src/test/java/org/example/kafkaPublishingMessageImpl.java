package org.example;

import Utils.kafkaUtils;
import com.thoughtworks.gauge.Step;

public class kafkaPublishingMessageImpl {

    kafkaUtils ku = new kafkaUtils();

    @Step("Create kafkaApp connection with cloudkarafka")
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

    @Step("Consume latest message from topic <topic>")
    public void consumeLatestMessageFromTopics(String topic) {
        ku.getLatestMessageFromKafkaTopic(topic);
    }
//    @Step("Close Consumer")
//    public void consumeLatestMessageFromTopics() {
//        ku.closeConsumer();
//    }
}
