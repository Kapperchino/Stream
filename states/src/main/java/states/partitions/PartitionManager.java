package states.partitions;

import com.google.common.base.Strings;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import models.lombok.Partition;
import models.lombok.Segment;
import models.lombok.Topic;
import models.proto.record.RecordListOuterClass.RecordList;
import models.proto.record.RecordOuterClass.Record;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import states.FileStoreCommon;
import states.config.Config;
import states.entity.FileStore;

import java.nio.ByteBuffer;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Slf4j
public class PartitionManager {
    Map<String, Topic> topicMap = new ConcurrentHashMap<>();
    RaftPeerId raftPeerId;
    RaftProperties properties;
    public FileStore store;

    public PartitionManager(Supplier<RaftPeerId> idSupplier, RaftProperties properties) {
        this.store = new FileStore(idSupplier, properties);
        raftPeerId = idSupplier.get();
        this.properties = properties;
    }

    public Partition createPartition(String topicName, int id) {
        if (Strings.isNullOrEmpty(topicName)) {
            throw new NullPointerException();
        }
        var segmentMap = new ConcurrentHashMap<Integer, Segment>();
        var segment = Segment.builder()
                .relativePath(Paths.get(String.format("%s/%s/0", topicName, id)))
                .segmentId(0)
                .build();
        store.write(0, Paths.get(String.format("%s/%s/0", topicName, id)).toString(), true, true, 0, null);
        segmentMap.put(0, segment);
        if (!topicMap.containsKey(topicName)) {
            log.info("Creating topic {}", topicName);
            topicMap.put(topicName, Topic.createTopic(topicName));
        }
        var partition = Partition.builder()
                .partitionId(id)
                .offset(new AtomicInteger(0))
                .topic(topicName)
                .segmentMap(segmentMap)
                .recordInfoMap(new ConcurrentHashMap<>())
                .build();
        var topic = topicMap.get(topicName);
        log.info("Adding partition {}", partition);
        topic.addPartition(partition);
        return partition;
    }

    @SneakyThrows
    public void writeToPartition(long index, String topicName, int id, List<Record> records) {
        if (Strings.isNullOrEmpty(topicName) || store == null) {
            throw new NullPointerException();
        }
        if (records.isEmpty()) {
            return;
        }
        var partition = getPartition(topicName, id);
        var segment = partition.getLastSegment();
        var curSegFileLeft = Config.MAX_SIZE_PER_SEG - partition.getLastRecord().getFileOffset();
        int offset = partition.getLastRecord().getFileOffset();

        if (partition.getLastRecord() == null) {
            curSegFileLeft = Config.MAX_SIZE_PER_SEG;
            offset = 0;
        }

        ByteBuffer buffer = ByteBuffer.allocate(Config.MAX_SIZE_PER_SEG);
        //write to cur file
        int i = 0;
        while (i < records.size() && records.get(i).getSerializedSize() < curSegFileLeft) {
            var curRec = records.get(i);
            buffer.put(curRec.toByteArray());
            partition.putRecordInfo(curRec, offset, segment.getSegmentId());
            curSegFileLeft -= curRec.getSerializedSize();
            i++;
        }
        store.write(0, segment.getRelativePath().toString(), false, true, 0, ByteString.copyFrom(buffer));
        //write to file
        if (i >= records.size() - 1) {
            store.close();
            return;
        }
        //iterate through other records
        buffer.clear();
        for (int x = i; i < records.size(); x++) {
            segment = partition.addSegment();
            //fill the buffer up
            while (buffer.position() + records.get(x).getSerializedSize() < Config.MAX_SIZE_PER_SEG) {
                buffer.put(records.get(x).toByteArray());
                partition.putRecordInfo(records.get(x), offset, segment.getSegmentId());
            }
            store.write(0, segment.getRelativePath().toString(), false, true, 0, ByteString.copyFrom(buffer));
            buffer.clear();
        }
        store.close();
    }

    @SneakyThrows
    public RecordList readFromPartition(long index, String topicName, int id, int offset) {
        if (Strings.isNullOrEmpty(topicName) || store == null) {
            throw new NullPointerException();
        }
        var partition = getPartition(topicName, id);
        var segment = partition.getSegment(offset);
        try (SeekableByteChannel in = Files.newByteChannel(segment.getRelativePath(), StandardOpenOption.READ)) {
            final ByteBuffer buffer = ByteBuffer.allocateDirect(FileStoreCommon.getChunkSize(Config.MAX_CHUNK));
            in.position(offset).read(buffer);
            buffer.flip();
            return RecordList.parseFrom(buffer);
        }
    }

    public Partition getPartition(String topicName, int id) {
        if (Strings.isNullOrEmpty(topicName)) {
            throw new NullPointerException();
        }
        if (!topicMap.containsKey(topicName)) {
            throw new NoSuchElementException(topicName);
        }
        if (!topicMap.get(topicName).getPartitionMap().containsKey(id)) {
            throw new NoSuchElementException(String.format("partition %s does not exist", id));
        }
        return topicMap.get(topicName).getPartitionMap().get(id);
    }


}
