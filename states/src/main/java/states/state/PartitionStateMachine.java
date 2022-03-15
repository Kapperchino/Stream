package states.state;

import lombok.SneakyThrows;
import models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import models.proto.requests.PublishRequestOuterClass.PublishRequest;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.*;
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
        this.partitionManager.createPartition("Test", 0);
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
        final ByteString content = request.getMessage().getContent();
        final PublishRequest proto = PublishRequest.parseFrom(content.toByteArray());
        final TransactionContext.Builder b = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request);
        final PublishRequest newProto = PublishRequest.newBuilder()
                .setHeader(proto.getHeader()).build();
        b.setLogData(ByteString.copyFrom(newProto.toByteArray())).setStateMachineData(ByteString.copyFrom(proto.getData().toByteArray()));
        return b.build();
    }

    @Override
    public CompletableFuture<Integer> write(LogEntryProto entry) {
        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final ByteString data = smLog.getLogData();
        final PublishRequest proto;
        try {
            proto = PublishRequest.parseFrom(data.toByteArray());
        } catch (Exception e) {
            return FileStoreCommon.completeExceptionally(
                    entry.getIndex(), "Failed to parse data, entry=" + entry, e);
        }
        var header = proto.getHeader();
        return partitionManager.writeToPartition(entry.getIndex(), header.getTopic(), 0, proto.getData().getDataList());
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
        LOG.info("linking {}", stream);
        return files.streamLink(stream);
    }

    @SneakyThrows
    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        final LogEntryProto entry = trx.getLogEntry();

        final long index = entry.getIndex();
        updateLastAppliedTermIndex(entry.getTerm(), index);

        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final PublishRequest request;
        request = PublishRequest.parseFrom(smLog.getLogData());
        return writeCommit(index, request.getHeader(), smLog.getStateMachineEntry().getStateMachineData().size());
    }

    private CompletableFuture<Message> writeCommit(
            long index, PublishRequestHeader header, int size) {
        return partitionManager.submitCommit(index, header, size)
                .thenApply(reply -> Message.valueOf(reply.toByteString()));
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
