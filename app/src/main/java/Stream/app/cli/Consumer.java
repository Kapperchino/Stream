package Stream.app.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
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
import stream.client.ConsumerClient;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Consumer cli for streams")
@Slf4j
public class Consumer extends Client {

    @Parameter(names = {"--topic", "--t"}, description = "Topic to consume records from", required = true)
    private String topic = null;

    @Parameter(names = {"--partition", "--p"}, description = "Partition to consume records from", required = true)
    private long partition = 0;

    @Parameter(names = {"--offset", "--o"}, description = "Offset to start consume records from", required = true)
    private long offset = 0;


    @Override
    protected void streamOperation(List<BaseClient> clients) throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        dropCache();
        log.info("Starting Async read now ");

        long startTime = System.currentTimeMillis();
        ConsumerClient firstClient = (ConsumerClient) clients.get(0);
        var result = firstClient.readPartition(offset, partition, topic);
        for(var record : result.getDataList()){
            log.info("Results: {}", record);
        }
        long endTime = System.currentTimeMillis();

        stopClients(clients);
    }

    @Override
    protected List<BaseClient> getClients(RaftProperties raftProperties,int numClients){
        List<BaseClient> consumerClients = new ArrayList<>();
        for (int i = 0; i < numClients; i++) {
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
            consumerClients.add(new ConsumerClient(client));
        }
        return consumerClients;
    }
}
