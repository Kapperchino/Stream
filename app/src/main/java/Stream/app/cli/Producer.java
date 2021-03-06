package Stream.app.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import stream.client.BaseClient;
import stream.client.PartitionClient;
import stream.client.ProducerClient;
import stream.models.proto.record.RecordOuterClass.Record;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Producer cli for streams")
@Slf4j
public class Producer extends Client {

    @Parameter(names = {"--file"}, description = "Proto file for the producer", required = false)
    private String path = null;

    @Parameter(names = {"--topic", "--t"}, description = "Topic to produce records to", required = true)
    private String topic = null;

    @Parameter(names = {"--records", "--r"}, description = "number of records", required = true)
    private int numRec = 0;

    @Override
    protected void streamOperation(List<BaseClient> clients) throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        dropCache();
        log.info("Starting Async write now ");

        long startTime = System.currentTimeMillis();
        var resultListBuilder = ImmutableList.builder();
        var producerClient = (ProducerClient) clients.get(0);
        var partitionClient = (PartitionClient) clients.get(1);

        var partitionOut = partitionClient.addPartition(topic, 0);
        log.info("Added partition: {}", partitionOut);

        var listBuilder = ImmutableList.<Record>builder();
        for (int i = 0; i < numRec; i++) {
            var builder = Record.newBuilder();
            builder.setKey(Integer.toString(i));
            byte[] b = new byte[200];
            new Random().nextBytes(b);
            builder.setPayload(ByteString.copyFrom(b));
            builder.setTopic(topic);
            listBuilder.add(builder.build());
        }
        resultListBuilder.add(producerClient.publish(listBuilder.build(), topic));
        var resultList = resultListBuilder.build();
        log.info("Results: {}", resultList);
        long endTime = System.currentTimeMillis();

        stopClients(clients);
    }

    @Override
    protected List<BaseClient> getClients(RaftProperties raftProperties, int numClients) {
        List<BaseClient> producerClients = new ArrayList<>();
        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                getPeers());

        RaftClient.Builder builder =
                RaftClient.newBuilder().setProperties(raftProperties);
        builder.setRaftGroup(raftGroup);
        builder.setClientRpc(
                new GrpcFactory(new org.apache.ratis.conf.Parameters())
                        .newRaftClientRpc(ClientId.randomId(), raftProperties));
        RaftPeer[] peers = getPeers();
        builder.setPrimaryDataStreamServer(peers[0]);
        RaftClient client = builder.build();
        producerClients.add(new ProducerClient(client));
        producerClients.add(new PartitionClient(client));
        return producerClients;
    }
}
