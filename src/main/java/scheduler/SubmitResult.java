package scheduler;

public enum SubmitResult {
    ACCEPTED,
    DUPLICATE,
    SERVER_BUSY,
    NO_AVAILABLE_WORKER,
    INVALID_TASK,
    DISPATCH_UNKNOWN
}
