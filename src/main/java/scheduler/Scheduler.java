package scheduler;

import java.util.concurrent.*;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;

public class Scheduler {
    private final ThreadPoolExecutor workerPool;
    private final ThreadPoolExecutor connectionPool;
    private final ConcurrentHashMap<String, Task> tasks;

    public Scheduler(){
        this.workerPool = new ThreadPoolExecutor(
                3,
                5,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(10),
                new ThreadPoolExecutor.AbortPolicy()
        );

        this.tasks = new ConcurrentHashMap<>();

        this.connectionPool = new ThreadPoolExecutor(
                2,
                4,
                60,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(20),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    public SubmitResult submitTask(Task task) {
        Task existing = tasks.putIfAbsent(
                task.getTaskId(),
                task
        );

        if (existing != null) {
            return SubmitResult.DUPLICATE;
        }

        try {

            workerPool.submit(() -> executeTask(task));

            return SubmitResult.ACCEPTED;

        } catch (RejectedExecutionException e) {

            tasks.remove(task.getTaskId(), task);

            return SubmitResult.SERVER_BUSY;
        }

    }

    private void executeTask(Task task) {
        String threadName = Thread.currentThread().getName();

        System.out.println(threadName + " starts " + task.getTaskId());

        task.setStatus(TaskStatus.RUNNING);

        try {
            Thread.sleep(task.getDurationMs());
            task.setStatus(TaskStatus.SUCCESS);

            System.out.println(threadName + "completed" + task.getTaskId());
        } catch (InterruptedException e) {
            task.setStatus(TaskStatus.FAILED);
            Thread.currentThread().interrupt();
        }
    }

    public TaskStatus getTaskStatus(String taskId) {
        Task task = tasks.get(taskId);
        if (task == null) {
            return null;
        }
        return task.getStatus();
    }

    public void start(int port) throws IOException {
        ServerSocket serverSocket = new ServerSocket(port); //listen on TCP connection

        System.out.println("Scheduler started on port " + port);

        while (true) {
            Socket clientSocket = serverSocket.accept(); //connection endpoint with some client

            connectionPool.submit(() -> handleClient(clientSocket));
        }
    }

    private void handleClient(Socket socket) {

        try (
                socket;

                BufferedReader reader = new BufferedReader(
                        new InputStreamReader(
                                socket.getInputStream()
                        )
                );

                PrintWriter writer = new PrintWriter(
                        socket.getOutputStream(),
                        true
                )
        ) {

            String request = reader.readLine();

            if (request == null) {
                return;
            }

            System.out.println("Received: " + request);

            String[] parts = request.split("\\|");

            if (parts.length != 3) {
                writer.println("ERROR|INVALID_REQUEST");
                return;
            }

            if (!parts[0].equals("SUBMIT")) {
                writer.println("ERROR|UNKNOWN_COMMAND");
                return;
            }

            String taskId = parts[1];

            int durationMs;

            try {
                durationMs = Integer.parseInt(parts[2]);
            } catch (NumberFormatException e) {
                writer.println("ERROR|INVALID_DURATION");
                return;
            }

            Task task = new Task(taskId, durationMs);

            SubmitResult result = submitTask(task);

            writer.println(
                    result + "|" + taskId
            );

        } catch (IOException e) {

            System.out.println(
                    "Client connection error: "
                            + e.getMessage()
            );
        }
    }

}
