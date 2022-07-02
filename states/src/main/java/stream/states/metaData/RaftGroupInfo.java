package stream.states.metaData;

import io.scalecube.cluster.Member;
import lombok.Builder;
import lombok.Data;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;

import java.util.List;
import java.util.Map;

@Builder
@Data
public class RaftGroupInfo {
    RaftGroup group;
    Member member;

    public static RaftGroupInfo of(RaftGroupId id, List<RaftPeer> peers, Member member) {
        var raftGroup = RaftGroup.valueOf(id, peers);
        return RaftGroupInfo.builder()
                .group(raftGroup)
                .member(member)
                .build();
    }
}
