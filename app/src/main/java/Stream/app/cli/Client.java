package Stream.app.cli;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import stream.client.BaseClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Client to connect filestore example cluster.
 */
public abstract class Client extends SubCommandBase {

    private static final int MAX_THREADS_NUM = 1000;
    private final long fileSizeInBytes = 1024;
    private final int bufferSizeInBytes = 1024;
    private final int numFiles = 1000;
    private final int numClients = 1;

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

        streamOperation(getClients(raftProperties, numClients));
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

    protected void dropCache() {
        String[] cmds = {"/bin/sh", "-c", "echo 3 > /proc/sys/vm/drop_caches"};
        try {
            Process pro = Runtime.getRuntime().exec(cmds);
            pro.waitFor();
        } catch (Throwable t) {
            System.err.println("Failed to run command:" + Arrays.toString(cmds) + ":" + t.getMessage());
        }
    }

    protected abstract void streamOperation(List<BaseClient> clients)
            throws IOException, ExecutionException, InterruptedException;
}
