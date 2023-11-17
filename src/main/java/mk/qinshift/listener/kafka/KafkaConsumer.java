package mk.qinshift.listener.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.TopicPartition;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.ConsumerSeekAware;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@EnableKafka
@Slf4j
public class KafkaConsumer implements ConsumerSeekAware {

    @KafkaListener(topicPattern = "test-project")
    public void receive(String payload, @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName) {
        try {
            log.info("Received kafka message for topic '{}'", topicName);
            log.info("Kafka message:\n{}", payload);
        } catch (Exception e) {
            log.error(e.getMessage());
        }
    }

    @Override
    public void registerSeekCallback(ConsumerSeekCallback callback) {
        // to be used for random callback registration if needed
    }

    @Override
    public void onPartitionsAssigned(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
//        assignments.keySet().forEach(partition -> callback.seekToEnd(partition.topic(), partition.partition()));
    }

    @Override
    public void onIdleContainer(Map<TopicPartition, Long> assignments, ConsumerSeekCallback callback) {
        // to be used when seeking on idle container events if needed
    }
}
