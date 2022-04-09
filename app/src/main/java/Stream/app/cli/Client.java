package Stream.app.cli;

import com.beust.jcommander.Parameter;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.FileUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import stream.client.BaseClient;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.*;

/**
 * Client to connect filestore example cluster.
 */
public abstract class Client extends SubCommandBase {

    private static final int MAX_THREADS_NUM = 1000;
    private final long fileSizeInBytes = 1024;
    private final int bufferSizeInBytes = 1024;
    private final int numFiles = 1000;
    private final int numClients = 1;
    @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
            required = true)
    private List<File> storageDir = new ArrayList<>();

    public int getNumThread() {
        return numFiles < MAX_THREADS_NUM ? numFiles : MAX_THREADS_NUM;
    }

    public long getFileSizeInBytes() {
        return fileSizeInBytes;
    }

    public int getBufferSizeInBytes() {
        return bufferSizeInBytes;
    }

    public int getNumFiles() {
        return numFiles;
    }

    @Override
    public void run() throws Exception {
        int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
        RaftProperties raftProperties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
        GrpcConfigKeys.setMessageSizeMax(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
                SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
        RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

        RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

        RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
                TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);

        for (File dir : storageDir) {
            FileUtils.createDirectories(dir);
        }

        streamOperation(getClients(raftProperties,numClients));
    }

    protected List<BaseClient> getClients(RaftProperties raftProperties, int numClients) {
        return null;
    }

    protected void stopClients(List<BaseClient> clients) throws IOException {
        for (var client : clients) {
            client.close();
        }
        System.exit(0);
    }

    public String getPath(String fileName) {
        int hash = fileName.hashCode() % storageDir.size();
        return new File(storageDir.get(Math.abs(hash)), fileName).getAbsolutePath();
    }

    protected void dropCache() {
        String[] cmds = {"/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"};
        try {
            Process pro = Runtime.getRuntime().exec(cmds);
            pro.waitFor();
        } catch (Throwable t) {
            System.err.println("Failed to run command:" + Arrays.toString(cmds) + ":" + t.getMessage());
        }
    }

    private CompletableFuture<Long> writeFileAsync(String path, ExecutorService executor) {
        final CompletableFuture<Long> future = new CompletableFuture<>();
        CompletableFuture.supplyAsync(() -> {
            try {
                future.complete(writeFile(path, fileSizeInBytes, bufferSizeInBytes));
            } catch (IOException e) {
                future.completeExceptionally(e);
            }
            return future;
        }, executor);
        return future;
    }

    protected List<String> generateFiles(ExecutorService executor) {
        UUID uuid = UUID.randomUUID();
        List<String> paths = new ArrayList<>();
        List<CompletableFuture<Long>> futures = new ArrayList<>();
        for (int i = 0; i < numFiles; i++) {
            String path = getPath("file-" + uuid + "-" + i);
            paths.add(path);
            futures.add(writeFileAsync(path, executor));
        }

        for (int i = 0; i < futures.size(); i++) {
            long size = futures.get(i).join();
            if (size != fileSizeInBytes) {
                System.err.println("Error: path:" + paths.get(i) + " write:" + size +
                        " mismatch expected size:" + fileSizeInBytes);
            }
        }

        return paths;
    }

    protected long writeFile(String path, long fileSize, long bufferSize) throws IOException {
        final byte[] buffer = new byte[Math.toIntExact(bufferSize)];
        long offset = 0;
        try (RandomAccessFile raf = new RandomAccessFile(path, "rw")) {
            while (offset < fileSize) {
                final long remaining = fileSize - offset;
                final long chunkSize = Math.min(remaining, bufferSize);
                ThreadLocalRandom.current().nextBytes(buffer);
                raf.write(buffer, 0, Math.toIntExact(chunkSize));
                offset += chunkSize;
            }
        }
        return offset;
    }

    protected abstract void streamOperation(List<BaseClient> clients)
            throws IOException, ExecutionException, InterruptedException;
}
