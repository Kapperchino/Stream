package states.state;

import lombok.extern.slf4j.Slf4j;
import models.proto.requests.AddPartitionRequestOuterClass.AddPartitionRequest;
import models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import models.proto.requests.WriteRequestOuterClass.WriteRequest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos;
import org.apache.ratis.proto.ExamplesProtos.*;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;
import states.FileStoreCommon;
import states.entity.FileStore;
import states.partitions.PartitionManager;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;

import static models.proto.requests.PublishRequestOuterClass.PublishRequest;

@Slf4j
public class PartitionStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    private final FileStore files;
    private final PartitionManager partitionManager;

    public PartitionStateMachine(RaftProperties properties) {
        this.partitionManager = new PartitionManager(this::getId, properties);
        files = partitionManager.store;
    }

    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
            throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        for (Path path : files.getRoots()) {
            FileUtils.createDirectories(path);
        }
    }

    @Override
    public StateMachineStorage getStateMachineStorage() {
        return storage;
    }

    @Override
    public void close() {
        files.close();
        setLastAppliedTermIndex(null);
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final ReadRequestProto proto;
        try {
            proto = ReadRequestProto.parseFrom(request.getContent());
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally("Failed to parse " + request, e);
        }

        final String path = proto.getPath().toStringUtf8();
        return files.read(path, proto.getOffset(), proto.getLength(), true)
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        log.info("incoming transactiono: {}", request);
        final ByteString content = request.getMessage().getContent();
        final var proto = WriteRequest.parseFrom(content.toByteArray());
        final TransactionContext.Builder b = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request);
        if (proto.getRequestCase() == WriteRequest.RequestCase.PUBLISH) {
            var publishProto = proto.getPublish();
            final WriteRequest newProto = WriteRequest.newBuilder()
                    .setPublish(PublishRequest.newBuilder().setHeader(publishProto.getHeader())).build();
            b.setLogData(ByteString.copyFrom(newProto.toByteArray())).setStateMachineData(ByteString.copyFrom(publishProto.getData().toByteArray()));
        } else {
            b.setLogData(content);
        }
        return b.build();
    }

    @Override
    public CompletableFuture<?> write(LogEntryProto entry) {
        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final ByteString data = smLog.getLogData();
        final WriteRequest proto;
        try {
            proto = WriteRequest.parseFrom(data.toByteArray());
        } catch (Exception e) {
            return FileStoreCommon.completeExceptionally(
                    entry.getIndex(), "Failed to parse data, entry=" + entry, e);
        }
        switch (proto.getRequestCase()) {
            case PUBLISH:
                var publishReq = proto.getPublish();
                var machineData = smLog.getStateMachineEntry().getStateMachineData();
                PublishRequestData publishData = null;
                try {
                    publishData = PublishRequestData.parseFrom(machineData);
                } catch (InvalidProtocolBufferException e) {
                    return FileStoreCommon.completeExceptionally(
                            entry.getIndex(), "Failed to parse data, entry=" + entry, e);
                }
                return partitionManager.writeToPartition(entry.getIndex(), publishReq.getHeader().getTopic(), 0, publishData);
            case ADDPARTITION:
                var addPartitionReq = proto.getAddPartition();
                return partitionManager.createPartition(addPartitionReq.getTopic(), addPartitionReq.getPartition());
            case CREATETOPIC:
                //not needed for now
                break;
            default:
                break;
        }
        return null;
        // sync only if closing the file
    }

    @Override
    public CompletableFuture<ByteString> read(LogEntryProto entry) {
        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final ByteString data = smLog.getLogData();
        final FileStoreRequestProto proto;
        try {
            proto = FileStoreRequestProto.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(
                    entry.getIndex(), "Failed to parse data, entry=" + entry, e);
        }
        if (proto.getRequestCase() != FileStoreRequestProto.RequestCase.WRITEHEADER) {
            return null;
        }

        final WriteRequestHeaderProto h = proto.getWriteHeader();
        CompletableFuture<ReadReplyProto> reply =
                files.read(h.getPath().toStringUtf8(), h.getOffset(), h.getLength(), false);

        return reply.thenApply(ReadReplyProto::getData);
    }

    @Override
    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
        final ByteString reqByteString = request.getMessage().getContent();
        final FileStoreRequestProto proto;
        try {
            proto = FileStoreRequestProto.parseFrom(reqByteString);
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(
                    "Failed to parse stream header", e);
        }
        return files.createDataChannel(proto.getStream().getPath().toStringUtf8())
                .thenApply(LocalStream::new);
    }

    @Override
    public CompletableFuture<?> link(DataStream stream, LogEntryProto entry) {
        log.info("linking {}", stream);
        return files.streamLink(stream);
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final LogEntryProto entry = trx.getLogEntry();

        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final WriteRequest request;
        try {
            request = WriteRequest.parseFrom(smLog.getLogData());
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(index,
                    "Failed to parse logData in" + smLog, e);
        }

        switch (request.getRequestCase()) {
            case PUBLISH:
                //add size calculation later
                return writeCommit(index, request.getPublish().getHeader(), request.getPublish().getData());
            case ADDPARTITION:
                return addPartitionCommit(index, request.getAddPartition());
            case CREATETOPIC:
                break;
            default:
                LOG.error(getId() + ": Unexpected request case " + request.getRequestCase());
                return FileStoreCommon.completeExceptionally(index,
                        "Unexpected request case " + request.getRequestCase());
        }
        return FileStoreCommon.completeExceptionally(index,
                "Unexpected request case " + request.getRequestCase());
    }


    private CompletableFuture<Message> writeCommit(
            long index, PublishRequestHeader header, PublishRequestData data) {
        var f1 = partitionManager
                .submitCommit(index, header, data);
        if (f1 != null) {
            return f1.thenApply(reply -> Message.valueOf(reply.toByteString()));
        }
        return null;
    }

    private CompletableFuture<Message> addPartitionCommit(
            long index, AddPartitionRequest request) {
        var f1 = partitionManager.submitAddPartition(index, request);
        if (f1 != null) {
            return f1.thenApply(reply -> Message.valueOf(reply.toByteString()));
        }
        return null;
    }

    private CompletableFuture<Message> streamCommit(StreamWriteRequestProto stream) {
        final String path = stream.getPath().toStringUtf8();
        final long size = stream.getLength();
        return files.streamCommit(path, size).thenApply(reply -> Message.valueOf(reply.toByteString()));
    }

    private CompletableFuture<Message> delete(long index, DeleteRequestProto request) {
        final String path = request.getPath().toStringUtf8();
        return files.delete(index, path).thenApply(resolved ->
                Message.valueOf(DeleteReplyProto.newBuilder().setResolvedPath(
                                FileStoreCommon.toByteString(resolved)).build().toByteString(),
                        () -> "Message:" + resolved));
    }

    static class LocalStream implements DataStream {
        private final DataChannel dataChannel;

        LocalStream(DataChannel dataChannel) {
            this.dataChannel = dataChannel;
        }

        @Override
        public DataChannel getDataChannel() {
            return dataChannel;
        }

        @Override
        public CompletableFuture<?> cleanUp() {
            return CompletableFuture.supplyAsync(() -> {
                try {
                    dataChannel.close();
                    return true;
                } catch (IOException e) {
                    return FileStoreCommon.completeExceptionally("Failed to close data channel", e);
                }
            });
        }
    }
}
