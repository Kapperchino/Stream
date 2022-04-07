package models.lombok;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import lombok.extern.jackson.Jacksonized;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Data
@Builder
@Jacksonized
public class Topic {
    @NonNull
    String name;
    @NonNull
    Map<Long, Partition> partitionMap;

    @JsonIgnore
    public int getNumPartitions() {
        return partitionMap.size();
    }

    @JsonIgnore
    public void addPartition(Partition partition) {
        if (partition == null) {
            throw new NullPointerException();
        }
        partitionMap.put(partition.partitionId, partition);
    }

    public Partition getPartition(long id) {
        return partitionMap.get(id);
    }

    public static Topic createTopic(String name) {
        return Topic.builder()
                .partitionMap(new ConcurrentHashMap<>())
                .name(name)
                .build();
    }
}
