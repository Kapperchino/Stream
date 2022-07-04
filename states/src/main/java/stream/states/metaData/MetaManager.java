package stream.states.metaData;

import com.google.common.collect.ImmutableList;
import io.scalecube.cluster.*;
import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.cluster.transport.api.Message;
import io.scalecube.net.Address;
import io.scalecube.transport.netty.tcp.TcpTransportFactory;
import lombok.Builder;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.com.google.protobuf.InvalidProtocolBufferException;
import stream.models.lombok.Topic;
import stream.models.proto.meta.ClusterMetaOuterClass;
import stream.models.proto.meta.ShardGroupMetaOuterClass;
import stream.models.proto.meta.ShardGroupMetaOuterClass.ShardGroupMeta;
import stream.models.proto.meta.TopicOuterClass;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Slf4j
public class MetaManager {
    private final int SEED_PORT = 6969;
    //meta-data for the current raft group
    ClusterMeta clusterMeta;
    ShardGroupInfo shardMeta;
    Map<String, TopicMeta> topicMap;
    private Cluster gossipCluster;
    private final RaftGroupId groupId;
    private final List<RaftPeer> peers;
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    public MetaManager(RaftGroupId raftGroupId, List<RaftPeer> peers) {
        topicMap = new ConcurrentHashMap<>();
        this.groupId = raftGroupId;
        clusterMeta = ClusterMeta.builder()
                .raftGroups(new ConcurrentSkipListMap<>())
                .build();
        this.peers = peers;
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
            shardMeta = ShardGroupInfo.of(groupId, peers, gossipCluster.member());
        }
    }

    public CompletableFuture<ClusterMetaOuterClass.ClusterMeta> getClusterMeta() {
        var builder = ClusterMetaOuterClass.ClusterMeta.newBuilder();
        builder.setTimestamp(clusterMeta.getTimeStamp().get());
        clusterMeta.getRaftGroups()
                .forEach((key, val) -> builder.putNodes(key, ShardGroupMeta.newBuilder()
                        .setGroupId(val.getGroup().getGroupId().getUuid().toString())
                        .addAllPeers(val.group.getPeers().stream()
                                .map((val1) -> ShardGroupMetaOuterClass.RaftPeer.newBuilder()
                                        .setRaftPeerId(val1.getId().toString())
                                        .setAddress(val1.getAddress())
                                        .build())
                                .collect(Collectors.toList()))
                        .build()));
        return CompletableFuture.supplyAsync(builder::build);
    }

    public CompletableFuture<List<TopicOuterClass.Topic>> getTopics() {
        return CompletableFuture.supplyAsync(() -> {
            var list = ImmutableList.<TopicOuterClass.Topic>builder();
            topicMap.forEach((key, val) -> {
                var builder = TopicOuterClass.Topic.newBuilder();
                builder.setTopicName(key);
                val.topic.getPartitionMap()
                        .forEach((key1, val1) -> builder.putPartitions(key1, TopicOuterClass.Partition.newBuilder()
                                .setTopic(val.getTopic().getName())
                                .setId(val1.getPartitionId())
                                .build()));
                list.add(builder.build());
            });
            return list.build();
        });
    }

    private void onMembershipChange(MembershipEvent event) throws InvalidProtocolBufferException {
        switch (event.type()) {
            case ADDED:
                var otherMeta = event.newMetadata();
                var proto = ShardGroupMeta.parseFrom(otherMeta);
                log.info("new member added: {}", proto);
                var peersList = proto.getPeersList().stream()
                        .map((val) -> {
                            var builder = RaftPeer.newBuilder();
                            builder.setId(val.getRaftPeerId());
                            builder.setAddress(val.getAddress());
                            return builder.build();
                        }).collect(Collectors.toList());
                clusterMeta.addRaftGroup(peersList, proto.getGroupId(), event.member(), event.timestamp());
                break;
            case REMOVED:
                var otherMeta1 = event.newMetadata();
                var proto2 = ShardGroupMeta.parseFrom(otherMeta1);
                clusterMeta.removeRaftGroup(proto2.getGroupId());
                break;
            case UPDATED:
                break;
        }
    }

    private Cluster createGossipCluster() {
        var isSeed = System.getenv("IS_SEED");
        var seedDNS = System.getenv("SEED_DNS");
        var peersProto = peers.stream().map((val) ->
                        ShardGroupMetaOuterClass.RaftPeer.newBuilder()
                                .setAddress(val.getAddress())
                                .setRaftPeerId(String.valueOf(val.getId()))
                                .build())
                .collect(Collectors.toList());
        var proto = ShardGroupMeta.newBuilder()
                .addAllPeers(peersProto)
                .setGroupId(groupId.getUuid().toString()).build();
        if (isSeed != null) {
            var configWithFixedPort =
                    new ClusterConfig()
                            .memberAlias(groupId.toString())
                            .metadata(proto.toByteString())
                            .metadataCodec(new ProtoCodec())
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
        var config =
                new ClusterConfig()
                        .memberAlias(groupId.toString())
                        .metadataCodec(new ProtoCodec())
                        .metadata(proto.toByteString());
        var cluster = new ClusterImpl()
                .config(opts -> config)
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
                try {
                    onMembershipChange(event);
                } catch (InvalidProtocolBufferException e) {
                    throw new RuntimeException(e);
                }
                executor.scheduleAtFixedRate(() -> gossipCluster
                                .spreadGossip(Message.fromData("joe biden"))
                                .doOnError(System.err::println)
                                .subscribe(null, Throwable::printStackTrace)
                        , 10, 10, TimeUnit.SECONDS);
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
                log.info("gossip message: {}", element);
            }
        };
    }

    @Builder
    @Value
    private static class TopicMeta {
        HashRing ring;
        Topic topic;
    }

}
