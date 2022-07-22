package stream.states.metaData;

import io.scalecube.cluster.Member;
import lombok.Builder;
import lombok.Data;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import stream.models.lombok.TopicPartition;

import java.util.List;
import java.util.Set;

@Builder
@Data
public class ShardGroupInfo {
    RaftGroup group;
    Member member;
    Set<TopicPartition> topicPartitions;

    public static ShardGroupInfo of(RaftGroupId id, List<RaftPeer> peers, Member member) {
        var raftGroup = RaftGroup.valueOf(id, peers);
        return ShardGroupInfo.builder()
                .group(raftGroup)
                .member(member)
                .build();
    }
}
