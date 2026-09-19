package scheduler;

import java.util.concurrent.*;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.InetSocketAddress;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Scheduler {
    private final ThreadPoolExecutor connectionPool;
    private final ConcurrentHashMap<String, Task> tasks;
    private final ConcurrentHashMap<String, WorkerInfo> workers;
    private final ScheduledExecutorService failureDetector;

    private static final long WORKER_TIMEOUT_MS = 5000;

    public Scheduler(){
        this.tasks = new ConcurrentHashMap<>();
        this.workers = new ConcurrentHashMap<>();
        this.failureDetector =
                Executors.newSingleThreadScheduledExecutor();

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
        if (task.getTaskId().isBlank() || task.getTaskId().contains("|")
                || task.getTaskId().contains("\n") || task.getTaskId().contains("\r")
                || task.getDurationMs() < 0) {
            return SubmitResult.INVALID_TASK;
        }
        Task existing = tasks.putIfAbsent(
                task.getTaskId(),
                task
        );

        if (existing != null) {
            return SubmitResult.DUPLICATE;
        }

        // This first version chooses one live worker; it does not retry another worker.
        WorkerInfo worker = workers.values().stream()
                .filter(w -> w.getStatus() == WorkerStatus.ALIVE)
                .findFirst().orElse(null);
        if (worker == null) {
            tasks.remove(task.getTaskId(), task);
            return SubmitResult.NO_AVAILABLE_WORKER;
        }
        return dispatchTask(worker, task);
    }

    private SubmitResult dispatchTask(WorkerInfo worker, Task task) {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(worker.getHost(), worker.getPort()), 2000);
            socket.setSoTimeout(2000);
            PrintWriter writer = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            writer.println("EXECUTE|" + task.getTaskId() + "|" + task.getDurationMs());
            String response = reader.readLine();
            if (("ACCEPTED|" + task.getTaskId()).equals(response)) {
                // Acceptance is not proof that execution has started or completed.
                task.setStatus(TaskStatus.DISPATCHED);
                return SubmitResult.ACCEPTED;
            }
            if (("SERVER_BUSY|" + task.getTaskId()).equals(response)) {
                tasks.remove(task.getTaskId(), task);
                return SubmitResult.SERVER_BUSY;
            }
        } catch (IOException e) {
            System.out.println("Dispatch uncertainty for " + task.getTaskId() + ": " + e.getMessage());
        }
        // Keep the ID reserved: a lost ACK does not mean the worker did not execute.
        task.setStatus(TaskStatus.DISPATCH_UNKNOWN);
        return SubmitResult.DISPATCH_UNKNOWN;
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

        startFailureDetector();

        while (true) {
            Socket clientSocket = serverSocket.accept(); //connection endpoint with some client

            try {
                clientSocket.setSoTimeout(3000);
                connectionPool.execute(() -> handleClient(clientSocket));
            } catch (RejectedExecutionException | IOException e) {
                clientSocket.close();
            }
        }
    }

    private void handleClient(Socket socket) {

        try (
                socket;

                BufferedReader reader =
                        new BufferedReader(
                                new InputStreamReader(
                                        socket.getInputStream()
                                )
                        );

                PrintWriter writer =
                        new PrintWriter(
                                socket.getOutputStream(),
                                true
                        )
        ) {

            String request = reader.readLine();

            if (request == null) {
                return;
            }

            System.out.println(
                    "Received: " + request
            );

            String[] parts =
                    request.split("\\|", -1);

            String command = parts[0];

            switch (command) {

                case "SUBMIT":
                    handleSubmit(parts, writer);
                    break;

                case "REGISTER":
                    handleRegister(parts, writer);
                    break;

                case "HEARTBEAT":
                    handleHeartbeat(parts, writer);
                    break;

                default:
                    writer.println("ERROR|UNKNOWN_COMMAND");
            }

        } catch (IOException e) {

            System.out.println(
                    "Client connection error: "
                            + e.getMessage()
            );
        }
    }

    private boolean registerWorker(
            String workerId,
            String host,
            int port
    ) {

        WorkerInfo worker = new WorkerInfo(
                workerId,
                host,
                port
        );

        WorkerInfo existing = workers.putIfAbsent(
                workerId,
                worker
        );

        return existing == null;
    }

    private void handleRegister(
            String[] parts,
            PrintWriter writer
    ) {

        if (parts.length != 4) {
            writer.println("ERROR|INVALID_REGISTER");
            return;
        }

        String workerId = parts[1];
        String host = parts[2];

        int port;

        try {
            port = Integer.parseInt(parts[3]);
        } catch (NumberFormatException e) {
            writer.println("ERROR|INVALID_PORT");
            return;
        }

        if (port < 1 || port > 65535 || workerId.isBlank() || host.isBlank()) {
            writer.println("ERROR|INVALID_REGISTER");
            return;
        }

        boolean registered =
                registerWorker(workerId, host, port);

        if (registered) {
            writer.println(
                    "REGISTERED|" + workerId
            );

            System.out.println(
                    "Worker registered: "
                            + workerId
                            + " at "
                            + host
                            + ":"
                            + port
            );

        } else {

            writer.println(
                    "ALREADY_REGISTERED|" + workerId
            );
        }
    }

    private void handleSubmit(
            String[] parts,
            PrintWriter writer
    ) {

        if (parts.length != 3) {
            writer.println("ERROR|INVALID_SUBMIT");
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

        Task task = new Task(
                taskId,
                durationMs
        );

        SubmitResult result =
                submitTask(task);

        writer.println(
                result + "|" + taskId
        );
    }

    private void handleHeartbeat(
            String[] parts,
            PrintWriter writer
    ) {

        if (parts.length != 2) {
            writer.println("ERROR|INVALID_HEARTBEAT");
            return;
        }

        String workerId = parts[1];

        WorkerInfo worker =
                workers.get(workerId);

        if (worker == null) {
            writer.println(
                    "ERROR|WORKER_NOT_REGISTERED"
            );
            return;
        }

        worker.updateHeartbeat();

        if (
                worker.getStatus()
                        == WorkerStatus.SUSPECTED_DEAD
        ) {

            worker.setStatus(
                    WorkerStatus.ALIVE
            );

            System.out.println(
                    "Worker recovered: "
                            + workerId
            );
        }

        writer.println(
                "HEARTBEAT_ACK|" + workerId
        );
    }

    private void startFailureDetector() {

        failureDetector.scheduleAtFixedRate(
                this::checkWorkerHealth,
                0,
                1,
                TimeUnit.SECONDS
        );
    }

    private void checkWorkerHealth() {

        long now =
                System.currentTimeMillis();

        for (WorkerInfo worker : workers.values()) {

            long elapsed =
                    now - worker.getLastHeartbeat();

            if (
                    elapsed > WORKER_TIMEOUT_MS
                            &&
                            worker.getStatus()
                                    == WorkerStatus.ALIVE
            ) {

                worker.setStatus(
                        WorkerStatus.SUSPECTED_DEAD
                );

                System.out.println(
                        "Worker suspected dead: "
                                + worker.getWorkerId()
                );
            }
        }
    }

}
