package stream.states.handlers;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import stream.models.proto.requests.WriteRequestOuterClass;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public interface TransactionHandler {
    TransactionContext startTransaction(RaftClientRequest request, WriteRequestOuterClass.WriteRequest proto, TransactionContext.Builder contextBuilder);

    CompletableFuture<Message> applyTransaction(TransactionContext trx);
}
