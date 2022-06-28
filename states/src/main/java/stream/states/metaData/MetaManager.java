package stream.states.metaData;

import dagger.Module;
import dagger.Provides;
import lombok.extern.slf4j.Slf4j;
import stream.models.lombok.Topic;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class MetaManager {
    ClusterMeta meta;
    Map<String, Topic> topicMap;

    public MetaManager() {
        topicMap = new ConcurrentHashMap<>();
    }

    //need to add the topic meta-data, also will need to talk to other groups to spread the partitions
    public void addTopic(String topic, int partitions){

    }
}
