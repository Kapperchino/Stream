package stream.states.metaData.handlers;

import lombok.Builder;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.statemachine.TransactionContext;
import stream.models.proto.requests.WriteRequestOuterClass;
import stream.models.proto.requests.WriteRequestOuterClass.WriteRequest;
import stream.states.handlers.TransactionHandler;
import stream.states.metaData.MetaManager;

import java.util.concurrent.CompletableFuture;

@Builder
public class MetaDataTransactionHandler implements TransactionHandler {
    MetaManager manager;

    @Override
    public TransactionContext startTransaction(RaftClientRequest request, WriteRequest proto, TransactionContext.Builder contextBuilder) {
        return null;
    }

    @Override
    public CompletableFuture<Message> applyTransaction(WriteRequestOuterClass.WriteRequest request, long index) {
        switch (request.getRequestCase()) {
            case CREATETOPIC:
                var req = request.getCreateTopic();
                return CompletableFuture.supplyAsync(() ->
                        Message.valueOf(manager.addTopic(req.getTopic(), (int) req.getPartitions()).toByteString()));
            default:
                return null;
        }
    }

}
