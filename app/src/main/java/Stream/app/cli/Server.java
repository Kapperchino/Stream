package Stream.app.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import io.scalecube.cluster.Cluster;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.ConfUtils;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.netty.NettyConfigKeys;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.apache.ratis.util.NetUtils;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import stream.states.StreamCommon;
import stream.states.state.PartitionStateMachine;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Class to start a ratis filestore example server.
 */
@Slf4j
@Parameters(commandDescription = "Start an filestore server")
public class Server extends SubCommandBase {

    @Parameter(names = {"--id", "-i"}, description = "Raft id of this server", required = true)
    private String id;

    @Parameter(names = {"--storage", "-s"}, description = "Storage dir, eg. --storage dir1 --storage dir2",
            required = true)
    private List<File> storageDir = new ArrayList<>();

    @Parameter(names = {"--writeThreadNum"}, description = "Number of write thread")
    private int writeThreadNum = 20;

    @Parameter(names = {"--readThreadNum"}, description = "Number of read thread")
    private int readThreadNum = 20;

    @Parameter(names = {"--commitThreadNum"}, description = "Number of commit thread")
    private int commitThreadNum = 3;

    @Parameter(names = {"--deleteThreadNum"}, description = "Number of delete thread")
    private int deleteThreadNum = 3;

    private final String SEED_NODE = "seed";

    private Cluster cluster;

    private final int SEED_PORT = 6969;

    @Override
    public void run() throws Exception {
        RaftPeerId peerId = RaftPeerId.valueOf(id);
        RaftProperties properties = new RaftProperties();

        // Avoid leader change affect the performance
        RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(2, TimeUnit.SECONDS));
        RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));

        final int port = NetUtils.createSocketAddr(getPeer(peerId).getAddress()).getPort();
        GrpcConfigKeys.Server.setPort(properties, port);

        Optional.ofNullable(getPeer(peerId).getClientAddress()).ifPresent(address ->
                GrpcConfigKeys.Client.setPort(properties, NetUtils.createSocketAddr(address).getPort()));
        Optional.ofNullable(getPeer(peerId).getAdminAddress()).ifPresent(address ->
                GrpcConfigKeys.Admin.setPort(properties, NetUtils.createSocketAddr(address).getPort()));

        String dataStreamAddress = getPeer(peerId).getDataStreamAddress();
        if (dataStreamAddress != null) {
            final int dataStreamport = NetUtils.createSocketAddr(dataStreamAddress).getPort();
            NettyConfigKeys.DataStream.setPort(properties, dataStreamport);
            RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.NETTY);
        }
        RaftServerConfigKeys.setStorageDir(properties, storageDir);
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, true);
        RaftServerConfigKeys.Write.setElementLimit(properties, 40960);
        RaftServerConfigKeys.Write.setByteLimit(properties, SizeInBytes.valueOf("1000MB"));
        ConfUtils.setFiles(properties::setFiles, StreamCommon.STATEMACHINE_DIR_KEY, storageDir);
        RaftServerConfigKeys.DataStream.setAsyncRequestThreadPoolSize(properties, writeThreadNum);
        RaftServerConfigKeys.DataStream.setAsyncWriteThreadPoolSize(properties, writeThreadNum);
        ConfUtils.setInt(properties::setInt, StreamCommon.STATEMACHINE_WRITE_THREAD_NUM, writeThreadNum);
        ConfUtils.setInt(properties::setInt, StreamCommon.STATEMACHINE_READ_THREAD_NUM, readThreadNum);
        ConfUtils.setInt(properties::setInt, StreamCommon.STATEMACHINE_COMMIT_THREAD_NUM, commitThreadNum);
        ConfUtils.setInt(properties::setInt, StreamCommon.STATEMACHINE_DELETE_THREAD_NUM, deleteThreadNum);
        StateMachine stateMachine = new PartitionStateMachine(properties);

        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                getPeers());
        RaftServer raftServer = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(id))
                .setStateMachine(stateMachine).setProperties(properties)
                .setGroup(raftGroup)
                .build();
        raftServer.start();

        while (raftServer.getLifeCycleState() != LifeCycle.State.CLOSED) {
            log.info("members in cluster: {}", cluster.members());
            TimeUnit.SECONDS.sleep(1);
        }
    }
}
