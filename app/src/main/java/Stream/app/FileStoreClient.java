package Stream.app;

import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.DataStreamOutput;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.ExamplesProtos.DeleteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.DeleteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.FileStoreRequestProto;
import org.apache.ratis.proto.ExamplesProtos.ReadReplyProto;
import org.apache.ratis.proto.ExamplesProtos.ReadRequestProto;
import org.apache.ratis.proto.ExamplesProtos.StreamWriteRequestProto;
import org.apache.ratis.proto.ExamplesProtos.WriteReplyProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestHeaderProto;
import org.apache.ratis.proto.ExamplesProtos.WriteRequestProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RoutingTable;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.JavaUtils;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.function.CheckedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import states.FileStoreCommon;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/** A standalone server using raft with a configurable state machine. */
public class FileStoreClient implements Closeable {
    public static final Logger LOG = LoggerFactory.getLogger(FileStoreClient.class);

    private final RaftClient client;

    public FileStoreClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .build();
    }

    public FileStoreClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setPrimaryDataStreamServer(primaryDataStreamServer)
                .build();
    }

    public FileStoreClient(RaftClient client) {
        this.client = client;
    }

    @Override
    public void close() throws IOException {
        client.close();
    }

    static ByteString send(
            ByteString request, CheckedFunction<Message, RaftClientReply, IOException> sendFunction)
            throws IOException {
        final RaftClientReply reply = sendFunction.apply(Message.valueOf(request));
        final StateMachineException sme = reply.getStateMachineException();
        if (sme != null) {
            throw new IOException("Failed to send request " + request, sme);
        }
        Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
        return reply.getMessage().getContent();
    }

    static CompletableFuture<ByteString> sendAsync(
            ByteString request, Function<Message, CompletableFuture<RaftClientReply>> sendFunction) {
        return sendFunction.apply(() -> request
        ).thenApply(reply -> {
            final StateMachineException sme = reply.getStateMachineException();
            if (sme != null) {
                throw new CompletionException("Failed to send request " + request, sme);
            }
            Preconditions.assertTrue(reply.isSuccess(), () -> "Failed " + request + ", reply=" + reply);
            return reply.getMessage().getContent();
        });
    }

    private ByteString send(ByteString request) throws IOException {
        return send(request, client.io()::send);
    }

    private ByteString sendReadOnly(ByteString request) throws IOException {
        return send(request, client.io()::sendReadOnly);
    }

    private CompletableFuture<ByteString> sendAsync(ByteString request) {
        return sendAsync(request, client.async()::send);
    }

    private CompletableFuture<ByteString> sendReadOnlyAsync(ByteString request) {
        return sendAsync(request, client.async()::sendReadOnly);
    }

    public ByteString read(String path, long offset, long length) throws IOException {
        final ByteString reply = readImpl(this::sendReadOnly, path, offset, length);
        return ReadReplyProto.parseFrom(reply).getData();
    }

    public CompletableFuture<ByteString> readAsync(String path, long offset, long length) {
        return readImpl(this::sendReadOnlyAsync, path, offset, length
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> ReadReplyProto.parseFrom(reply).getData()));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT readImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendReadOnlyFunction,
            String path, long offset, long length) throws THROWABLE {
        final ReadRequestProto read = ReadRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setOffset(offset)
                .setLength(length)
                .build();

        return sendReadOnlyFunction.apply(read.toByteString());
    }

    public long write(String path, long offset, boolean close, ByteBuffer buffer, boolean sync)
            throws IOException {
        final int chunkSize = FileStoreCommon.getChunkSize(buffer.remaining());
        buffer.limit(chunkSize);
        final ByteString reply = writeImpl(this::send, path, offset, close, buffer, sync);
        return WriteReplyProto.parseFrom(reply).getLength();
    }

    public DataStreamOutput getStreamOutput(String path, long dataSize, RoutingTable routingTable) {
        final StreamWriteRequestProto header = StreamWriteRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setLength(dataSize)
                .build();
        final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setStream(header).build();
        return client.getDataStreamApi().stream(request.toByteString().asReadOnlyByteBuffer(), routingTable);
    }

    public CompletableFuture<Long> writeAsync(String path, long offset, boolean close, ByteBuffer buffer, boolean sync) {
        return writeImpl(this::sendAsync, path, offset, close, buffer, sync
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> WriteReplyProto.parseFrom(reply).getLength()));
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT writeImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction,
            String path, long offset, boolean close, ByteBuffer data, boolean sync)
            throws THROWABLE {
        final WriteRequestHeaderProto.Builder header = WriteRequestHeaderProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path))
                .setOffset(offset)
                .setLength(data.remaining())
                .setClose(close)
                .setSync(sync);

        final WriteRequestProto.Builder write = WriteRequestProto.newBuilder()
                .setHeader(header)
                .setData(ByteString.copyFrom(data));

        final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setWrite(write).build();
        return sendFunction.apply(request.toByteString());
    }

    private static <OUTPUT, THROWABLE extends Throwable> OUTPUT deleteImpl(
            CheckedFunction<ByteString, OUTPUT, THROWABLE> sendFunction, String path)
            throws THROWABLE {
        final DeleteRequestProto.Builder delete = DeleteRequestProto.newBuilder()
                .setPath(ProtoUtils.toByteString(path));
        final FileStoreRequestProto request = FileStoreRequestProto.newBuilder().setDelete(delete).build();
        return sendFunction.apply(request.toByteString());
    }

    public String delete(String path) throws IOException {
        final ByteString reply = deleteImpl(this::send, path);
        return DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8();
    }

    public CompletableFuture<String> deleteAsync(String path) {
        return deleteImpl(this::sendAsync, path
        ).thenApply(reply -> JavaUtils.supplyAndWrapAsCompletionException(
                () -> DeleteReplyProto.parseFrom(reply).getResolvedPath().toStringUtf8()));
    }
}
