package io.github.stefan_zobel.freyja.client;

/**
 * A request did not do what was asked. The subclasses say what is known about it.
 */
public class FreyjaException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public FreyjaException(String message) {
        super(message);
    }

    public FreyjaException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * The time the request allowed has passed and nobody can say whether the command took
     * effect: it was sent, and no answer came. It may have been applied, and it may still be
     * applied later.
     */
    public static final class UnknownOutcome extends FreyjaException {
        private static final long serialVersionUID = 1L;

        public UnknownOutcome(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The time the request allowed has passed and the request had no effect: a read that was
     * not answered, or a command that no node took.
     */
    public static final class TimedOut extends FreyjaException {
        private static final long serialVersionUID = 1L;

        public TimedOut(String message, Throwable cause) {
            super(message, cause);
        }
    }

    /**
     * The cluster is short of space and accepts only deletes. The command had no effect and
     * may be sent again when an administrator has cleared the alarm.
     */
    public static final class NoSpace extends FreyjaException {
        private static final long serialVersionUID = 1L;

        public NoSpace(String message) {
            super(message);
        }
    }

    /** The request was refused for good: it is not valid, or the node does not offer it. */
    public static final class Refused extends FreyjaException {
        private static final long serialVersionUID = 1L;

        public Refused(String message) {
            super(message);
        }
    }
}
