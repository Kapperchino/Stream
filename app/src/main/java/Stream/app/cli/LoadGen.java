package Stream.app.cli;

import Stream.app.FileStoreClient;
import Stream.app.ProducerClient;
import Stream.app.cli.Client;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import org.apache.ratis.thirdparty.io.netty.buffer.ByteBuf;
import org.apache.ratis.thirdparty.io.netty.buffer.PooledByteBufAllocator;


import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Load Generator for FileStore")
public class LoadGen extends Client {

    @Parameter(names = {"--sync"}, description = "Whether sync every bufferSize", required = false)
    private int sync = 0;

    @Override
    protected void operation(List<FileStoreClient> clients) throws IOException, ExecutionException, InterruptedException {
        final ExecutorService executor = Executors.newFixedThreadPool(getNumThread());
        List<String> paths = generateFiles(executor);
        dropCache();
        System.out.println("Starting Async write now ");

        long startTime = System.currentTimeMillis();

        long totalWrittenBytes = waitWriteFinish(writeByHeapByteBuffer(paths, clients, executor));

        long endTime = System.currentTimeMillis();

        System.out.println("Total files written: " + getNumFiles());
        System.out.println("Each files size: " + getFileSizeInBytes());
        System.out.println("Total data written: " + totalWrittenBytes + " bytes");
        System.out.println("Total time taken: " + (endTime - startTime) + " millis");

        stop(clients);
    }

    @Override
    protected void streamOperation(List<ProducerClient> clients) throws IOException, ExecutionException, InterruptedException {

    }

    long write(FileChannel in, long offset, FileStoreClient fileStoreClient, String path,
               List<CompletableFuture<Long>> futures) throws IOException {
        final int bufferSize = getBufferSizeInBytes();
        final ByteBuf buf = PooledByteBufAllocator.DEFAULT.heapBuffer(bufferSize);
        final int bytesRead = buf.writeBytes(in, bufferSize);

        if (bytesRead < 0) {
            throw new IllegalStateException("Failed to read " + bufferSize + " byte(s) from " + this
                    + ". The channel has reached end-of-stream at " + offset);
        } else if (bytesRead > 0) {
            final CompletableFuture<Long> f = fileStoreClient.writeAsync(
                    path, offset, offset + bytesRead == getFileSizeInBytes(), buf.nioBuffer(),
                    sync == 1);
            f.thenRun(buf::release);
            futures.add(f);
        }
        return bytesRead;
    }

    private Map<String, CompletableFuture<List<CompletableFuture<Long>>>> writeByHeapByteBuffer(
            List<String> paths, List<FileStoreClient> clients, ExecutorService executor) {
        Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap = new HashMap<>();

        int clientIndex = 0;
        for (String path : paths) {
            final CompletableFuture<List<CompletableFuture<Long>>> future = new CompletableFuture<>();
            final FileStoreClient client = clients.get(clientIndex % clients.size());
            clientIndex++;
            CompletableFuture.supplyAsync(() -> {
                List<CompletableFuture<Long>> futures = new ArrayList<>();
                File file = new File(path);
                try (FileInputStream fis = new FileInputStream(file)) {
                    final FileChannel in = fis.getChannel();
                    for (long offset = 0L; offset < getFileSizeInBytes(); ) {
                        offset += write(in, offset, client, file.getName(), futures);
                    }
                } catch (Throwable e) {
                    future.completeExceptionally(e);
                }

                future.complete(futures);
                return future;
            }, executor);

            fileMap.put(path, future);
        }

        return fileMap;
    }

    private long waitWriteFinish(Map<String, CompletableFuture<List<CompletableFuture<Long>>>> fileMap)
            throws ExecutionException, InterruptedException {
        long totalBytes = 0;
        for (CompletableFuture<List<CompletableFuture<Long>>> futures : fileMap.values()) {
            long writtenLen = 0;
            for (CompletableFuture<Long> future : futures.get()) {
                writtenLen += future.join();
            }

            if (writtenLen != getFileSizeInBytes()) {
                System.out.println("File written:" + writtenLen + " does not match expected:" + getFileSizeInBytes());
            }

            totalBytes += writtenLen;
        }
        return totalBytes;
    }
}
