package stream.states.state;

import io.scalecube.cluster.Cluster;
import io.scalecube.cluster.ClusterConfig;
import io.scalecube.cluster.ClusterImpl;
import io.scalecube.cluster.ClusterMessageHandler;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.DeleteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.DeleteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.FileStoreRequestProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteRequestProto;
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
import org.apache.ratis.thirdparty.com.google.protobuf.AbstractMessageLite;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import org.apache.ratis.util.FileUtils;
import stream.models.proto.requests.AddPartitionRequestOuterClass.AddPartitionRequest;
import stream.models.proto.requests.ConsumeRequestOuterClass.ConsumeRequest;
import stream.models.proto.requests.PublishRequestDataOuterClass.PublishRequestData;
import stream.models.proto.requests.PublishRequestHeaderOuterClass.PublishRequestHeader;
import stream.models.proto.requests.ReadRequestOuterClass.ReadRequest;
import stream.models.proto.requests.WriteRequestOuterClass.WriteRequest;
import stream.models.proto.responses.ConsumeResponseOuterClass.ConsumeResponse;
import stream.states.FileStoreCommon;
import stream.states.entity.FileStore;
import stream.states.metaData.MetaManager;
import stream.states.partitions.PartitionManager;
import stream.states.snapshot.SnapshotHelper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static stream.models.proto.requests.PublishRequestOuterClass.PublishRequest;

@Slf4j
public class PartitionStateMachine extends BaseStateMachine {
    private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

    private final FileStore files;
    public PartitionManager partitionManager;
    private final AtomicBoolean isLeader;
    private final MetaManager metaManager;

    public PartitionStateMachine(RaftProperties properties) {
        this.partitionManager = new PartitionManager(this::getId, properties);
        this.metaManager = new MetaManager(getGroupId(), getId());
        files = partitionManager.store;
        isLeader = new AtomicBoolean(false);
    }

    @Override
    public void notifyLeaderChanged(RaftGroupMemberId groupMemberId, RaftPeerId newLeaderId) {
        //case where the member becomes leader
        if (!isLeader.get()) {
            if (newLeaderId == this.getId()) {
                isLeader.compareAndSet(false, true);
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
    public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage)
            throws IOException {
        super.initialize(server, groupId, raftStorage);
        this.storage.init(raftStorage);
        Files.createDirectories(files.resolve(Path.of("MetaData")));
        for (Path path : files.getRoots()) {
            FileUtils.createDirectories(path);
        }
        SnapshotHelper.loadSnapShot(this, storage, false).get();
    }

    @SneakyThrows
    @Override
    public void reinitialize() throws IOException {
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
            return FileStoreCommon.completeExceptionally(
                    "Failed to parse data, entry=" + request, e);
        }
        if (proto.getRequestCase() != ReadRequest.RequestCase.CONSUME) {
            return null;
        }

        final ConsumeRequest consume = proto.getConsume();
        CompletableFuture<ConsumeResponse> reply =
                partitionManager.readFromPartition(consume.getTopic(),
                        (int) consume.getPartition(), (int) consume.getOffset());

        return reply.thenApply((a) -> Message.valueOf(a.toByteString()));
    }

    @Override
    public TransactionContext startTransaction(RaftClientRequest request) throws IOException {
        log.info("incoming transaction: {}", request);
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
        if (proto.getRequestCase() == WriteRequest.RequestCase.PUBLISH) {
            var publishReq = proto.getPublish();
            var machineData = smLog.getStateMachineEntry().getStateMachineData();
            PublishRequestData publishData = null;
            try {
                publishData = PublishRequestData.parseFrom(machineData);
            } catch (InvalidProtocolBufferException e) {
                return FileStoreCommon.completeExceptionally(
                        entry.getIndex(), "Failed to parse data, entry=" + entry, e);
            }
            //TODO: partition management will be added after we get publishing, consuming working with one node and two replicas
            return partitionManager.writeToPartition(entry.getIndex(), publishReq.getHeader().getTopic(), 0, publishData);
        }
        return null;
    }

    @Override
    public CompletableFuture<ByteString> read(LogEntryProto entry) {
        final StateMachineLogEntryProto smLog = entry.getStateMachineLogEntry();
        final ByteString data = smLog.getLogData();
        final ReadRequest proto;
        try {
            proto = ReadRequest.parseFrom(data);
        } catch (InvalidProtocolBufferException e) {
            return FileStoreCommon.completeExceptionally(
                    entry.getIndex(), "Failed to parse data, entry=" + entry, e);
        }
        if (proto.getRequestCase() != ReadRequest.RequestCase.CONSUME) {
            return null;
        }

        final ConsumeRequest request = proto.getConsume();
        CompletableFuture<ConsumeResponse> reply =
                partitionManager.readFromPartition(request.getTopic(),
                        (int) request.getPartition(), (int) request.getOffset());

        return reply.thenApply(AbstractMessageLite::toByteString);
    }

    //TODO: add streaming
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
            return FileStoreCommon.completeExceptionally(index,
                    "Failed to parse logData in" + smLog, e);
        }

        switch (request.getRequestCase()) {
            case PUBLISH:
                //TODO: add recovery features, currently when the state machines are down we lose all meta-data
                return writeCommit(index, request.getPublish().getHeader(), request.getPublish().getData());
            case ADDPARTITION:
                return addPartition(index, request.getAddPartition());
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
        return FileStoreCommon.completeExceptionally(
                index, "Failed to commit, index: " + index);
    }

    private CompletableFuture<Message> addPartition(
            long index, AddPartitionRequest request) {
        var f1 = partitionManager.addPartition(index, request);
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
                    return FileStoreCommon.completeExceptionally("Failed to close data channel", e);
                }
            });
        }
    }
}
