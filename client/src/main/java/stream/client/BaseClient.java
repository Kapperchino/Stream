package stream.client;

import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.Preconditions;
import org.apache.ratis.util.function.CheckedFunction;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.function.Function;

/**
 * A standalone server using raft with a configurable state machine.
 */
//todo: make config another package
@Slf4j
public class BaseClient implements Closeable {
    protected final RaftClient client;

    public BaseClient(RaftGroup group, RaftProperties properties)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .build();
    }

    public BaseClient(RaftGroup group, RaftProperties properties, RaftPeer primaryDataStreamServer)
            throws IOException {
        this.client = RaftClient.newBuilder()
                .setProperties(properties)
                .setRaftGroup(group)
                .setPrimaryDataStreamServer(primaryDataStreamServer)
                .build();
    }

    public BaseClient(RaftClient client) {
        this.client = client;
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

    @Override
    public void close() throws IOException {
        client.close();
    }

    protected ByteString send(ByteString request) throws IOException {
        return send(request, client.io()::send);
    }

    protected ByteString sendReadOnly(ByteString request) throws IOException {
        return send(request, client.io()::sendReadOnly);
    }

    protected CompletableFuture<ByteString> sendAsync(ByteString request) {
        return sendAsync(request, client.async()::send);
    }

    protected CompletableFuture<ByteString> sendReadOnlyAsync(ByteString request) {
        return sendAsync(request, client.async()::sendReadOnly);
    }
}
