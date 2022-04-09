package stream.states.partitions;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import stream.models.lombok.Partition;
import stream.models.lombok.Segment;
import stream.models.lombok.Topic;
import stream.models.lombok.dto.FileWrittenMeta;
import stream.models.lombok.dto.WriteFileMeta;
import stream.models.lombok.dto.WriteResultFutures;
import stream.models.proto.record.RecordMetaOuterClass.RecordMeta;
import stream.models.proto.record.RecordOuterClass.Record;
import stream.models.proto.requests.AddPartitionRequestOuterClass.AddPartitionRequest;
import stream.models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import stream.models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import stream.models.proto.responses.AddPartitionResponseOuterClass.AddPartitionResponse;
import stream.models.proto.responses.ConsumeResponseOuterClass.ConsumeResponse;
import stream.models.proto.responses.PublishResponseOuterClass.PublishResponse;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.CodedOutputStream;
import org.apache.ratis.util.JavaUtils;
import stream.states.config.Config;
import stream.states.entity.FileStore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
@NoArgsConstructor
public class PartitionManager {
    //TODO: Serialize this map to a proto, load from MetaData folder at startup
    @JsonProperty
    Map<String, Topic> topicMap = new ConcurrentHashMap<>();
    @JsonIgnore
    Map<Long, ConcurrentLinkedQueue<FileWrittenMeta>> commitMap = new ConcurrentHashMap<>();
    @JsonProperty
    RaftProperties properties;
    @JsonProperty
    public FileStore store;

    public PartitionManager(Supplier<RaftPeerId> idSupplier, RaftProperties properties) {
        this.store = new FileStore(idSupplier, properties);
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
                .relativePath(String.format("%s/%s/0", topicName, id))
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

        ByteArrayOutputStream stream = new ByteArrayOutputStream(Config.MAX_SIZE_PER_SEG);
        var offset = startingOffset;
        //if cur file is full, then create a new file and resetting stuff
        if (records.get(0).getSerializedSize() + offset > Config.MAX_SIZE_PER_SEG) {
            segment.setSize(offset);
            segment = partition.addSegment();
            offset = 0;
        }
        //iterate through other records
        var builder = ImmutableList.<WriteFileMeta>builder();
        for (int x = 0; x < records.size(); x++) {
            //fill the buffer up
            startingOffset = offset;
            //writes until buffer full or all records written
            while (x < records.size() && offset + getRecordSize(records.get(x)) < Config.MAX_SIZE_PER_SEG) {
                var record = records.get(x);
                record.writeDelimitedTo(stream);
                partition.putRecordInfo(record, offset, segment.getSegmentId());
                offset += getRecordSize(record);
                x++;
            }
            x--;
            segment.setSize(offset);
            var shouldClose = offset >= Config.MAX_SIZE_PER_SEG;
            var writeFile = WriteFileMeta.builder()
                    .index((int) index)
                    .path(segment.getRelativePath())
                    .close(shouldClose)
                    .sync(true)
                    .offset(startingOffset)
                    .data(ByteString.copyFrom(stream.toByteArray()))
                    .build();
            var fileMeta = writeFile.getFileWritten(stream.size());
            commitQueue.offer(fileMeta);
            builder.add(writeFile);
            if (x < records.size()) {
                segment = partition.addSegment();
            }
            offset = 0;
            stream.reset();
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
        var fileList = ImmutableList.<Segment>builder();

        var partition = getPartition(topicName, id);
        var startingSegment = partition.getSegment(offset);
        var startingRecord = partition.getRecord(offset);
        var initialSegSize = Config.MAX_SIZE_PER_SEG - startingRecord.getFileOffset();
        var sizeLeft = Config.MAX_SIZE_PER_SEG - initialSegSize;
        fileList.add(startingSegment);
        int i = offset;
        while (sizeLeft > 0) {
            var nextSeg = partition.getSegment(++i);
            if (nextSeg == null) {
                break;
            }
            sizeLeft -= Config.MAX_SIZE_PER_SEG;
            fileList.add(nextSeg);
        }
        int fileOffset = startingRecord.getFileOffset();
        var futures = ImmutableList.<CompletableFuture<ReadReplyProto>>builder();
        for (var seg : fileList.build()) {
            var f =
                    store.read(seg.getRelativePath(), fileOffset, seg.getSize() - fileOffset, true);
            futures.add(f);
        }
        var results = ConsumeResponse.newBuilder();
        //now that we have the files, we need to read through them and parse to record
        for (var future : futures.build()) {
            var buffer = future.get().getData();
            var input = buffer.newInput();
            var record = Record.parseDelimitedFrom(input);
            while (record != null) {
                results.addData(record);
                record = Record.parseDelimitedFrom(input);
            }
        }
        return CompletableFuture.supplyAsync(results::build);
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

    private int getRecordSize(Record record) {
        int size = record.getSerializedSize();
        return size + CodedOutputStream.computeUInt32SizeNoTag(size);
    }

}
