package states.partitions;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import models.lombok.Partition;
import models.lombok.Segment;
import models.lombok.Topic;
import models.lombok.dto.FileWrittenMeta;
import models.lombok.dto.WriteFileMeta;
import models.lombok.dto.WriteResultFutures;
import models.proto.record.RecordMetaOuterClass.RecordMeta;
import models.proto.requests.AddPartitionRequestOuterClass.AddPartitionRequest;
import models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import models.proto.responses.AddPartitionResponseOuterClass.AddPartitionResponse;
import models.proto.responses.ConsumeResponseOuterClass.ConsumeResponse;
import models.proto.responses.PublishResponseOuterClass.PublishResponse;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import states.FileStoreCommon;
import states.config.Config;
import states.entity.FileStore;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

@Slf4j
public class PartitionManager {
    //TODO: Serialize this map to a proto, load from MetaData folder at startup
    Map<String, Topic> topicMap = new ConcurrentHashMap<>();
    Map<Long, ConcurrentLinkedQueue<FileWrittenMeta>> commitMap = new ConcurrentHashMap<>();
    RaftPeerId raftPeerId;
    RaftProperties properties;
    public FileStore store;

    public PartitionManager(Supplier<RaftPeerId> idSupplier, RaftProperties properties) {
        this.store = new FileStore(idSupplier, properties);
        raftPeerId = idSupplier.get();
        this.properties = properties;
    }

    @SneakyThrows
    public Partition createPartition(long index, String topicName, long id) {
        if (Strings.isNullOrEmpty(topicName)) {
            throw new NullPointerException();
        }
        if (topicMap.containsKey(topicName)) {
            var topic = topicMap.get(topicName);
            if (topic.getPartition(id) != null) {
                return topic.getPartition(id);
            }
        }
        var segmentMap = new ConcurrentHashMap<Integer, Segment>();
        var segment = Segment.builder()
                .relativePath(Paths.get(String.format("%s/%s/0", topicName, id)))
                .segmentId(0)
                .build();
        var fileMeta = WriteFileMeta.builder()
                .data(null)
                .offset(0)
                .path(Paths.get(String.format("%s/%s/0", topicName, id)).toString())
                .close(true)
                .sync(true)
                .build();

        Files.createDirectories(store.resolve(Path.of(fileMeta.getPath()).getParent()));
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
        log.info("Index: {}, Created a new partition {}", index, partition);
        return partition;
    }

    @SneakyThrows
    public CompletableFuture<WriteResultFutures> writeToPartition(long index, String topicName, int id, PublishRequestData data) {
        if (Strings.isNullOrEmpty(topicName) || data == null) {
            throw new NullPointerException();
        }
        var records = data.getDataList();
        if (records.isEmpty()) {
            return null;
        }
        var partition = getPartition(topicName, id);
        var segment = partition.getLastSegment();
        int startingOffset = 0;
        if (partition.getLastRecord() != null) {
            var lastRecord = partition.getLastRecord();
            startingOffset = lastRecord.getFileOffset() + lastRecord.getSize();
        }
        var commitQueue = new ConcurrentLinkedQueue<FileWrittenMeta>();
        commitMap.put(index, commitQueue);

        ByteBuffer buffer = ByteBuffer.allocate(Config.MAX_SIZE_PER_SEG);
        var offset = startingOffset;
        //if cur file is full, then create a new file and resetting stuff
        if (records.get(0).getSerializedSize() + offset > Config.MAX_SIZE_PER_SEG) {
            segment = partition.addSegment();
            offset = 0;
        }
        //iterate through other records
        var builder = ImmutableList.<WriteFileMeta>builder();
        for (int x = 0; x < records.size(); x++) {
            //fill the buffer up
            startingOffset = offset;
            while (x < records.size() && offset + records.get(x).getSerializedSize() < Config.MAX_SIZE_PER_SEG) {
                buffer.put(records.get(x).toByteArray());
                partition.putRecordInfo(records.get(x), offset, segment.getSegmentId());
                offset += records.get(x).getSerializedSize();
                x++;
            }
            var shouldClose = offset >= Config.MAX_SIZE_PER_SEG;
            buffer.flip();
            var writeFile = WriteFileMeta.builder()
                    .index((int) index)
                    .path(segment.getRelativePath().toString())
                    .close(shouldClose)
                    .sync(true)
                    .offset(startingOffset)
                    .data(ByteString.copyFrom(buffer))
                    .build();
            var fileMeta = writeFile.getFileWritten(offset);
            commitQueue.offer(fileMeta);
            builder.add(writeFile);
            if (x < records.size()) {
                segment = partition.addSegment();
            }
            offset = 0;
            buffer.clear();
        }
        var list = builder.build();
        var res = store.write(list);
        return CompletableFuture.supplyAsync(() -> res);
    }

    public CompletableFuture<AddPartitionResponse> addPartition(long index, AddPartitionRequest request) {
        var partition = createPartition(index, request.getTopic(), request.getPartition());

        return CompletableFuture.supplyAsync(() ->
                        AddPartitionResponse.newBuilder().setTopic(request.getTopic()).setPartitionId(request.getPartition()).build())
                .whenComplete((val, t) -> log.info("Partition {} created", val));
    }

    //TODO: handle errors
    public CompletableFuture<PublishResponse> submitCommit(long index, PublishRequestHeader header, PublishRequestData data) {
        if (!commitMap.containsKey(index)) {
            return JavaUtils.completeExceptionally(
                    new IOException(index + " is already committed."));
        }
        var queue = commitMap.get(index);
        var builder = ImmutableList.<CompletableFuture<Integer>>builder();
        log.info("Committing files from the commit queue");
        while (!queue.isEmpty()) {
            var meta = queue.poll();
            builder.add(store.submitCommit(index, meta.getPath(), meta.isClose(), meta.getOffset(), (int) meta.getSize()));
        }
        commitMap.remove(index);
        var list = builder.build();
        for (var future : list) {
            //todo handle the errors
            if (future.isCompletedExceptionally()) {
                log.error("Error committing write:");
            } else {
                try {
                    var res = future.join();
                    log.info("Written {} bytes to topic: {} and partition: {}", res, header.getTopic(), 0);
                } catch (Exception e) {
                    log.error("Error committing write: ", e);
                }
            }
        }
        //TODO: get offset from map
        return CompletableFuture.supplyAsync(() -> {
            var response = PublishResponse.newBuilder();
            for (var record : data.getDataList()) {
                response.addData(RecordMeta.newBuilder()
                        .setKey(record.getKey())
                        .setOffset(0)
                        .setTopic(record.getTopic()));
            }
            log.info("Write commit finished, with response {}", response.build());
            return response.build();
        });
    }

    //read min(max chunk, 1000 records)
    @SneakyThrows
    public CompletableFuture<ConsumeResponse> readFromPartition(String topicName, int id, int offset) {
        if (Strings.isNullOrEmpty(topicName) || store == null) {
            throw new NullPointerException();
        }
        var partition = getPartition(topicName, id);
        var lastRec = partition.getLastRecord();
        var startingRec = partition.getRecord(offset + 1);
        if (startingRec == null) {
            return FileStoreCommon.completeExceptionally(-1,
                    "No messages to read", new NoSuchElementException());
        }
        var maxNumRec = Math.min(lastRec.getOffset() - offset, Config.MAX_RECORD_READ);
        var bytesToRead = calculateBytesToRead(maxNumRec, startingRec.getOffset(), partition);
        var startingOffset = startingRec.getOffset();
        var startingSeg = partition.getSegment(startingOffset).getSegmentId();
        var endingSeg = partition.getSegment(startingOffset + maxNumRec).getSegmentId();
        var listBuilder = ImmutableList.<CompletableFuture<ReadReplyProto>>builder();
        for (int x = startingSeg; x < endingSeg; x++) {
            var curSeg = partition.getSegment(x);
            var f = store.read(curSeg.getRelativePath().toString(), startingRec.getFileOffset(),
                    Math.min(Config.MAX_RECORD_READ - startingRec.getFileOffset(), bytesToRead), true);
            listBuilder.add(f);
        }
        ByteBuffer buffer = ByteBuffer.allocate(Config.MAX_RECORD_READ);
        for (var f : listBuilder.build()) {
            buffer.put(f.get().getData().asReadOnlyByteBuffer());
        }
        buffer.flip();
        var result = ConsumeResponse.parseFrom(buffer);
        return CompletableFuture.supplyAsync(() -> result);
    }

    public Partition getPartition(String topicName, long id) {
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

    private int calculateBytesToRead(int numRecords, int startingOffset, Partition partition) {
        var endingRec = partition.getRecord(startingOffset + numRecords);
        var curSize = 0;
        for (int x = startingOffset; x < startingOffset + numRecords && curSize < Config.MAX_RECORD_READ; x++) {
            var info = partition.getRecord(x);
            curSize += info.getSize();
        }
        return curSize;
    }

}
