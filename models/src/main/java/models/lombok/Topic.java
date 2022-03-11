package models.lombok;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
public class Topic {
    String name;
    @NonNull
    Map<Integer, Partition> partitionMap;

    public int getNumPartitions() {
        return partitionMap.size();
    }

    public void addPartition(Partition partition) {
        if (partition == null) {
            throw new NullPointerException();
        }
        partitionMap.put(partition.partitionId, partition);
    }

    public static Topic createTopic(String name) {
        return Topic.builder()
                .partitionMap(new ConcurrentHashMap<>())
                .name(name)
                .build();
    }
}
