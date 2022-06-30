package stream.states.metaData;

import io.scalecube.cluster.*;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeerId;
import stream.models.lombok.Topic;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.*;

@Slf4j
public class MetaManager {
    private final int SEED_PORT = 6969;
    ClusterMeta meta;
    Map<String, Topic> topicMap;
    Map<String, Member> membershipMap;
    private Cluster gossipCluster;
    private final RaftGroupId groupId;
    private final RaftPeerId id;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public MetaManager(RaftGroupId raftGroupId, RaftPeerId id) {
        topicMap = new ConcurrentHashMap<>();
        membershipMap = new ConcurrentHashMap<>();
        this.id = id;
        this.groupId = raftGroupId;
    }

    //need to add the topic meta-data, also will need to talk to other groups to spread the partitions
    public void addTopic(String topic, int partitions) {

    }

    public void shutDown() {
        gossipCluster.shutdown();
        gossipCluster = null;
    }

    public void startGossipCluster() {
        if (gossipCluster == null) {
            gossipCluster = createGossipCluster();
        }
    }

    private void onMembershipChange(MembershipEvent event) {
        switch (event.type()) {
            case ADDED:
                membershipMap.put(event.member().alias(), event.member());
                break;
            case REMOVED:
                membershipMap.remove(event.member().alias());
                break;
            case UPDATED:
                break;
        }
    }

    private Cluster createGossipCluster() {
        var isSeed = System.getenv("IS_SEED");
        var seedDNS = System.getenv("SEED_DNS");
        if (isSeed != null) {
            var configWithFixedPort =
                    new ClusterConfig()
                            .memberAlias(groupId.toString())
                            .transport(opts -> opts.port(SEED_PORT));
            var cluster = new ClusterImpl()
                    .config(opts -> configWithFixedPort)
                    .transportFactory(TcpTransportFactory::new)
                    .handler((cluster1) -> getHandler());
            cluster.startAwait();
            log.info("Starting a seed cluster at: {}", cluster.address());
            return cluster;
        }
        //not a seed node
        var cluster = new ClusterImpl()
                .config(opts -> opts.memberAlias(groupId.toString()))
                .membership(opts -> opts.seedMembers(Address.from(seedDNS + ":" + SEED_PORT)))
                .transportFactory(TcpTransportFactory::new)
                .handler((c) -> getHandler())
                .startAwait();
        log.info("Joining a seed cluster from: {} to {}", cluster.address(), seedDNS);
        return cluster;
    }

    private ClusterMessageHandler getHandler() {
        return new ClusterMessageHandler() {
            @Override
            public void onMembershipEvent(MembershipEvent event) {
                log.info("membership event: {}", event);
                onMembershipChange(event);
                executor.scheduleAtFixedRate(() -> gossipCluster
                        .spreadGossip(Message.fromData("joe biden"))
                        .doOnError(System.err::println)
                        .subscribe(null, Throwable::printStackTrace)
                        ,10, 10, TimeUnit.SECONDS);
            }

            @Override
            public void onGossip(Message message) {
                log.info("On gossip: {}", message);
                var element = new ByteArrayOutputStream();
                ObjectOutputStream stream = null;
                try {
                    stream = new ObjectOutputStream(element);
                    message.writeExternal(stream);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                log.info("gossip message: {}", element.toString());
            }
        };
    }

}
