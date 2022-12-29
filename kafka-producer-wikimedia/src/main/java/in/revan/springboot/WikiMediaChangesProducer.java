package in.revan.springboot;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.util.concurrent.TimeUnit;

@Service
@Slf4j
public class WikiMediaChangesProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final NewTopic topic;

    public WikiMediaChangesProducer(KafkaTemplate<String, String> kafkaTemplate, NewTopic topic) {
        this.kafkaTemplate = kafkaTemplate;
        this.topic = topic;
    }

    public void sendMessage() throws InterruptedException {
        //to read real time stream data from wikimedia
        EventHandler eventHandler = new WikiMediaChangesHandler(kafkaTemplate, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource eventSource = new EventSource.Builder(eventHandler, URI.create(url)).build();

        eventSource.start();
        TimeUnit.MINUTES.sleep(10);

    }
}
