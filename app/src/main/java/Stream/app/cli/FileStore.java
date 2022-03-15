package Stream.app.cli;

import java.util.ArrayList;
import java.util.List;


/**
 * This class enumerates all the commands enqueued by FileStore state machine.
 */
public final class FileStore {
    private FileStore() {
    }

    public static List<SubCommandBase> getSubCommands() {
        List<SubCommandBase> commands = new ArrayList<>();
        commands.add(new Server());
        commands.add(new LoadGen());
        commands.add(new DataStream());
        commands.add(new Producer());
        return commands;
    }
}
