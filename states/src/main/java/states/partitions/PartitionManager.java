package states.partitions;

import models.lombok.Partition;
import models.lombok.Segment;
import models.proto.RecordOuterClass.Record;
import states.entity.FileStore;

import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class PartitionManager {
    public Partition createPartition(String topic, int id, FileStore store) {
        var segmentMap = new ConcurrentHashMap<Integer, Segment>();
        var segment = Segment.builder()
                .relativePath(Paths.get(String.format("%s/%s/0", topic, id)))
                .build();
        store.write(0, Paths.get(String.format("%s/%s/0", topic, id)).toString(), false, true, 0, null);
        segmentMap.put(0, segment);
        return Partition.builder()
                .partitionId(id)
                .offset(0)
                .topic(topic)
                .segmentMap(segmentMap)
                .recordInfoMap(new ConcurrentHashMap<>())
                .build();
    }

    public void writeToPartition(String topic, int id, List<Record> records, FileStore store){
    }



}
