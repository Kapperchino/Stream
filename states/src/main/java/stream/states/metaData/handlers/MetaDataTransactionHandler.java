package stream.states.metaData.handlers;

import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.states.handlers.TransactionHandler;

import java.util.concurrent.CompletableFuture;

public class MetaDataTransactionHandler implements TransactionHandler {
    @Override
    public TransactionContext startTransaction(RaftClientRequest request, WriteRequestOuterClass.WriteRequest proto, TransactionContext.Builder contextBuilder) {
        return null;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
        return null;
    }
}
