package io.github.sz.freyja.node.store;

import java.io.IOException;

/**
 * The node cannot know what it has promised: its identity file or both hard-state files are
 * missing or unreadable (docs/spec/node.md, section 3.4). The node must not start. The only
 * safe recovery is to remove the member and add a new one, with a new id, on an empty data
 * directory.
 */
public final class LostIdentityException extends IOException {

    private static final long serialVersionUID = 1L;

    public LostIdentityException(String message) {
        super(message);
    }
}
