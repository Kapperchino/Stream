package states.snapshot;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.util.AutoCloseableLock;
import states.partitions.PartitionManager;
import states.state.PartitionStateMachine;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReentrantReadWriteLock;


@Slf4j
public class SnapshotHelper {
    static ObjectMapper objectMapper = new ObjectMapper();

    static {
        objectMapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.DEFAULT);
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        objectMapper.configure(SerializationFeature.WRITE_SELF_REFERENCES_AS_NULL, true);
        objectMapper.configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    @SneakyThrows
    public static CompletableFuture<Long> takeSnapshot(PartitionManager manager, SimpleStateMachineStorage storage, TermIndex last) {
        var output = objectMapper.writeValueAsString(manager);
        return CompletableFuture.supplyAsync(() -> {
            final File snapshotFile = storage.getSnapshotFile(last.getTerm(), last.getIndex());
            try (var out = new BufferedOutputStream(new FileOutputStream(snapshotFile))) {
                out.write(output.getBytes(StandardCharsets.UTF_8));
            } catch (IOException ioe) {
                log.warn("Failed to write snapshot file \"" + snapshotFile
                        + "\", last applied index=" + last);
            }
            return last.getIndex();
        });
    }

    @SneakyThrows
    public static CompletableFuture<Long> loadSnapShot(PartitionStateMachine stateMachine, SimpleStateMachineStorage storage, boolean reload) {
        return load(stateMachine, storage.getLatestSnapshot(), reload);
    }

    private static CompletableFuture<Long> load(PartitionStateMachine stateMachine, SingleFileSnapshotInfo snapshot, boolean reload) throws IOException {
        if (snapshot == null) {
            log.warn("The snapshot info is null.");
            return CompletableFuture.completedFuture(RaftLog.INVALID_LOG_INDEX);
        }
        final File snapshotFile = snapshot.getFile().getPath().toFile();
        if (!snapshotFile.exists()) {
            log.warn("The snapshot file {} does not exist for snapshot {}", snapshotFile, snapshot);
            return CompletableFuture.completedFuture(RaftLog.INVALID_LOG_INDEX);
        }

        return CompletableFuture.supplyAsync(() -> {
            final TermIndex last = SimpleStateMachineStorage.getTermIndexFromSnapshotFile(snapshotFile);
            try (AutoCloseableLock writeLock = writeLock();
                 InputStream in = new BufferedInputStream(new FileInputStream(snapshotFile))) {
                if (reload) {
                    reset();
                }
                //TODO:Serialize here
                stateMachine.partitionManager = objectMapper.readValue(in, PartitionManager.class);
                stateMachine.setLastAppliedTermIndex(last);
            } catch (Exception e) {
                log.error("Issues loading snapshot: ", e);
                return RaftLog.INVALID_LOG_INDEX;
            }
            return last.getIndex();
        });
    }

    //TODO
    public static void reset() {
    }

    private static AutoCloseableLock writeLock() {
        return AutoCloseableLock.acquire(lock.writeLock());
    }

    private static final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);
}
