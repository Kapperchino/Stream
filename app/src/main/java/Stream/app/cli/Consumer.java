package Stream.app.cli;

import Stream.app.FileStoreClient;
import Stream.app.ProducerClient;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
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
    protected void operation(List<FileStoreClient> clients) throws IOException, ExecutionException, InterruptedException {

    }

    @Override
    protected void streamOperation(List<ProducerClient> clients) throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        dropCache();
        log.info("Starting Async read now ");

        long startTime = System.currentTimeMillis();
        var firstClient = clients.get(0);
        var result = firstClient.readPartition(offset, partition, topic);
        for(var record : result.getDataList()){
            log.info("Results: {}", record);
        }
        long endTime = System.currentTimeMillis();

        stopProducers(clients);
    }
}
