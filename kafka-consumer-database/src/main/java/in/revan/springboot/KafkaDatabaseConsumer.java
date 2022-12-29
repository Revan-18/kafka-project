package in.revan.springboot;

import in.revan.springboot.entity.WikiMediaData;
import in.revan.springboot.repository.WikimwdiaDataRepository;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaDatabaseConsumer {

    @Autowired
    private WikimwdiaDataRepository dataRepository;

    @KafkaListener(topics = "wikimedia_recentchange", groupId = "myGroup")
    public void consume(String eventMessage){
        log.info(String.format("Event Message Received ->  %s",eventMessage));
        WikiMediaData wikiMediaData = new WikiMediaData();
        wikiMediaData.setWikiEventData(eventMessage);

        dataRepository.save(wikiMediaData);

    }


}
