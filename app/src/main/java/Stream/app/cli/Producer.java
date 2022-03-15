package Stream.app.cli;

import Stream.app.FileStoreClient;
import Stream.app.ProducerClient;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import lombok.extern.slf4j.Slf4j;
import models.proto.record.RecordOuterClass.Record;

import java.io.IOException;
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


    @Override
    protected void operation(List<FileStoreClient> clients) throws IOException, ExecutionException, InterruptedException {

    }

    @Override
    protected void streamOperation(List<ProducerClient> clients) throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        dropCache();
        log.info("Starting Async write now ");

        long startTime = System.currentTimeMillis();
        var resultListBuilder = ImmutableList.builder();
        int x = 0;
        for (var client : clients) {
            var builder = Record.newBuilder();
            builder.setKey(Integer.toString(x++));
            byte[] b = new byte[20];
            new Random().nextBytes(b);
            builder.setPayload(ByteString.copyFrom(b));
            builder.setTopic(topic);
            var listBuilder = ImmutableList.<Record>builder();
            for (int i = 0; i < 10; i++) {
                listBuilder.add(builder.build());
            }
            resultListBuilder.add(client.publish(builder.getKey(), listBuilder.build(), "Test"));
        }
        var resultList = resultListBuilder.build();
        log.info("Results: {}", resultList);
        long endTime = System.currentTimeMillis();

        stopProducers(clients);
    }


}