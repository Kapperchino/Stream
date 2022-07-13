package stream.states.state;

import com.google.common.collect.ImmutableList;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.FileStoreRequestProto;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.proto.RaftProtos.StateMachineLogEntryProto;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;
import stream.models.proto.requests.ReadRequestOuterClass.ReadRequest;
import stream.models.proto.requests.WriteRequestOuterClass.WriteRequest;
import stream.states.StreamCommon;
import stream.states.entity.FileStore;
import stream.states.handlers.ReadHandler;
import stream.states.handlers.TransactionHandler;
import stream.states.handlers.WriteHandler;
import stream.states.metaData.MetaManager;
import stream.states.metaData.handlers.MetaDataReadHandler;
import stream.states.metaData.handlers.MetaDataTransactionHandler;
import stream.states.metaData.handlers.MetaDataWriteHandler;
import stream.states.partitions.PartitionManager;
import stream.states.partitions.handlers.PartitionReadHandler;
import stream.states.partitions.handlers.PartitionTransactionHandler;
import stream.states.partitions.handlers.PartitionWriteHandler;
import stream.states.snapshot.SnapshotHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
public class PartitionStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    private final FileStore files;
    public PartitionManager partitionManager;
    private final AtomicBoolean isLeader;
    private MetaManager metaManager;
    private RaftServer server;
    private RaftGroupId id;
    private final List<ReadHandler> readHandlers;
    private final List<WriteHandler> writeHandlers;
    private final List<TransactionHandler> transactionHandlers;

    public PartitionStateMachine(RaftProperties properties) {
        this.partitionManager = new PartitionManager(this::getId, properties);
        files = partitionManager.store;
        isLeader = new AtomicBoolean(false);
        readHandlers = ImmutableList.of(
                PartitionReadHandler
                        .builder()
                        .partitionManager(partitionManager)
                        .build(),
                MetaDataReadHandler
                        .builder()
                        .manager(metaManager)
                        .build());
        writeHandlers = ImmutableList.of(
                PartitionWriteHandler
                        .builder()
                        .manager(partitionManager)
                        .build(),
                MetaDataWriteHandler
                        .builder()
                        .manager(metaManager)
                        .build());
        transactionHandlers = ImmutableList.of(
                PartitionTransactionHandler
                        .builder()
                        .partitionManager(partitionManager)
                        .build(),
                MetaDataTransactionHandler
                        .builder()
                        .manager(metaManager)
                        .build());
    }

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        //case where the member becomes leader
        if (!isLeader.get()) {
            if (newLeaderId == this.getId()) {
                isLeader.compareAndSet(false, true);
                var list = ImmutableList.<RaftPeer>builder();
                try {
                    for (var group : server.getGroups()) {
                        list.addAll(group.getPeers());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                metaManager = new MetaManager(id, list.build(), partitionManager);
                metaManager.startGossipCluster();
            }
            //TODO: case where leader down, but comes back up and was a seed node
            // currently assuming seed node does not go down
        } else {
            //need to shut the cluster down
            if (newLeaderId != this.getId()) {
                isLeader.compareAndSet(true, false);
                metaManager.shutDown();
            }
        }
    }

    @SneakyThrows
    @Override
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        Files.createDirectories(files.resolve(Path.of("MetaData")));
        for (Path path : files.getRoots()) {
            FileUtils.createDirectories(path);
        }
        SnapshotHelper.loadSnapShot(this, storage, false).get();
        this.server = server;
        this.id = groupId;
    }

    @SneakyThrows
    @Override
    public void reinitialize() {
        close();
        SnapshotHelper.loadSnapShot(this, storage, false).get();
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

    @SneakyThrows
    @Override
    public long takeSnapshot() {
        var last = this.getLastAppliedTermIndex();
        var res = SnapshotHelper.takeSnapshot(partitionManager, storage, last);
        res.get();
        return last.getIndex();
    }

    @Override
    public CompletableFuture<Message> query(Message request) {
        final ReadRequest proto;
        try {
            proto = ReadRequest.parseFrom(request.getContent());
        } catch (InvalidProtocolBufferException e) {
            return StreamCommon.completeExceptionally(
                    "Failed to parse data, entry=" + request, e);
        }
        if (proto.getRequestCase() != ReadRequest.RequestCase.CONSUME) {
            return null;
        }
        var resBuilder = ImmutableList.<CompletableFuture<ByteString>>builder();
        readHandlers.forEach(val -> {
            var res = val.handleRead(proto);
            if (res != null) {
                resBuilder.add(res);
            }
        });

        var resList = resBuilder.build();
        if (resList.size() > 1) {
            log.error("More than one transaction was valid but only one should be valid");
            throw new RuntimeException("More than one transaction was valid but only one should be valid");
        }
        if (resList.isEmpty()) {
            return null;
        }

        return resList.get(0).thenApply(Message::valueOf);
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        log.info("incoming transaction: {}", request);
        final ByteString content = request.getMessage().getContent();
        final var proto = WriteRequest.parseFrom(content.toByteArray());
        final TransactionContext.Builder b = TransactionContext.newBuilder()
                .setStateMachine(this)
                .setClientRequest(request);
        var resBuilder = ImmutableList.<TransactionContext>builder();
        transactionHandlers.forEach(val -> {
            var res = val.startTransaction(request, proto, b);
            if (res != null) {
                resBuilder.add(res);
            }
        });
        var resList = resBuilder.build();
        if (resList.size() > 1) {
            log.error("More than one transaction was valid but only one should be valid");
            throw new RuntimeException("More than one transaction was valid but only one should be valid");
        }
        if (resList.isEmpty()) {
            b.setLogData(content);
            return b.build();
        }
        return resList.get(0);
    }

    @Override
    public CompletableFuture<?> write(LogEntryProto entry) {
        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final ByteString data = smLog.getLogData();
        final WriteRequest proto;
        try {
            proto = WriteRequest.parseFrom(data.toByteArray());
        } catch (Exception e) {
            return StreamCommon.completeExceptionally(
                    entry.getIndex(), "Failed to parse data, entry=" + entry, e);
        }
        var resBuilder = ImmutableList.<CompletableFuture<?>>builder();
        writeHandlers.forEach(val -> {
            var res = val.handleWrite(proto, entry);
            if (res != null) {
                resBuilder.add(res);
            }
        });
        var resList = resBuilder.build();
        if (resList.size() > 1) {
            log.error("More than one transaction was valid but only one should be valid");
            throw new RuntimeException("More than one transaction was valid but only one should be valid");
        }
        if (resList.isEmpty()) {
            return null;
        }
        return resList.get(0);
    }

    @Override
    public CompletableFuture<ByteString> read(LogEntryProto entry) {
        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final ByteString data = smLog.getLogData();
        final ReadRequest proto;
        try {
            proto = ReadRequest.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return StreamCommon.completeExceptionally(
                    entry.getIndex(), "Failed to parse data, entry=" + entry, e);
        }
        var resBuilder = ImmutableList.<CompletableFuture<ByteString>>builder();
        readHandlers.forEach(val -> {
            var res = val.handleRead(proto);
            if (res != null) {
                resBuilder.add(res);
            }
        });

        var resList = resBuilder.build();
        if (resList.size() > 1) {
            log.error("More than one transaction was valid but only one should be valid");
            throw new RuntimeException("More than one transaction was valid but only one should be valid");
        }
        if (resList.isEmpty()) {
            return null;
        }

        return resList.get(0);
    }

    //TODO: add streaming
    @Override
    public CompletableFuture<DataStream> stream(RaftClientRequest request) {
        final ByteString reqByteString = request.getMessage().getContent();
        final FileStoreRequestProto proto;
        try {
            proto = FileStoreRequestProto.parseFrom(reqByteString);
        } catch (InvalidProtocolBufferException e) {
            return StreamCommon.completeExceptionally(
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

    @SneakyThrows
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
            return StreamCommon.completeExceptionally(index,
                    "Failed to parse logData in" + smLog, e);
        }

        var resBuilder = ImmutableList.<CompletableFuture<Message>>builder();
        transactionHandlers.forEach(val -> {
            var res = val.applyTransaction(request, index);
            if (res != null) {
                resBuilder.add(res);
            }
        });

        var resList = resBuilder.build();
        if (resList.size() > 1) {
            log.error("More than one transaction was valid but only one should be valid");
            throw new RuntimeException("More than one transaction was valid but only one should be valid");
        }
        if (resList.isEmpty()) {
            return null;
        }

        return StreamCommon.completeExceptionally(index,
                "Unexpected request case " + request.getRequestCase());
    }

    public void setLastAppliedTermIndex(TermIndex newTI) {
        super.setLastAppliedTermIndex(newTI);
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
                    return StreamCommon.completeExceptionally("Failed to close data channel", e);
                }
            });
        }
    }
}
