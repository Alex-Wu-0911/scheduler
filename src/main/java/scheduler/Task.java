package scheduler;

public class Task {
    private final String taskId;
    private final int durationMs;
    private volatile TaskStatus status;

    public Task(String taskId, int durationMs) {
        this.taskId = taskId;
        this.durationMs = durationMs;
        this.status = TaskStatus.PENDING;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getDurationMs() {
        return durationMs;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }
}
