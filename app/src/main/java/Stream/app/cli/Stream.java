package Stream.app.cli;

import java.util.ArrayList;
import java.util.List;


/**
 * This class enumerates all the commands enqueued by FileStore state machine.
 */
public final class Stream {
    private Stream() {
    }

    public static List<SubCommandBase> getSubCommands() {
        List<SubCommandBase> commands = new ArrayList<>();
        commands.add(new Server());
        commands.add(new Producer());
        commands.add(new Consumer());
        return commands;
    }
}
