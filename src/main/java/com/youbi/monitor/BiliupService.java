package com.youbi.monitor;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Pattern;

@Service
public class BiliupService {
    private static final Pattern ANSI_PATTERN = Pattern.compile("\\u001B\\[[;?0-9]*[ -/]*[@-~]");

    private final Path binPath;
    private final Path cookiePath;
    private final String proxy;
    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Map<String, BiliupJob> jobs = new ConcurrentHashMap<>();

    public BiliupService(
            @Value("${biliup.bin:../uploader/ydbi_uploader/bin/biliup}") String binPath,
            @Value("${biliup.cookie:../uploader/ydbi_uploader/cookies.json}") String cookiePath,
            @Value("${biliup.proxy:}") String proxy
    ) {
        this.binPath = Path.of(binPath).toAbsolutePath().normalize();
        this.cookiePath = Path.of(cookiePath).toAbsolutePath().normalize();
        this.proxy = proxy == null ? "" : proxy.trim();
    }

    public BiliupStatus status() throws IOException {
        boolean cookieExists = Files.exists(cookiePath);
        long cookieSize = cookieExists ? Files.size(cookiePath) : 0;
        LocalDateTime cookieUpdatedAt = null;
        if (cookieExists) {
            Instant instant = Files.getLastModifiedTime(cookiePath).toInstant();
            cookieUpdatedAt = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
        }
        return new BiliupStatus(
                binPath.toString(),
                Files.exists(binPath),
                cookiePath.toString(),
                cookieExists,
                cookieSize,
                cookieUpdatedAt
        );
    }

    public BiliupJobSnapshot startLogin() {
        return startJob("login");
    }

    public BiliupJobSnapshot startRenew() {
        return startJob("renew");
    }

    public Optional<BiliupJobSnapshot> getJob(String id) {
        BiliupJob job = jobs.get(id);
        return job == null ? Optional.empty() : Optional.of(job.snapshot());
    }

    public Optional<BiliupJobSnapshot> sendInput(String id, String input) throws IOException {
        BiliupJob job = jobs.get(id);
        if (job == null) {
            return Optional.empty();
        }
        job.sendInput(input == null ? "" : input);
        return Optional.of(job.snapshot());
    }

    public Optional<BiliupJobSnapshot> cancelJob(String id) {
        BiliupJob job = jobs.get(id);
        if (job == null) {
            return Optional.empty();
        }
        job.cancel();
        return Optional.of(job.snapshot());
    }

    public BiliupCommandResult listVideos(BiliupVideoQuery query) throws IOException, InterruptedException {
        List<String> command = baseCommand();
        command.add("list");
        switch (query.type() == null ? "" : query.type()) {
            case "pubing" -> command.add("--is-pubing");
            case "pubed" -> command.add("--pubed");
            case "notPubed" -> command.add("--not-pubed");
            case "", "all" -> {
            }
            default -> throw new IllegalArgumentException("Unsupported video query type: " + query.type());
        }
        command.add("--from-page");
        command.add(String.valueOf(Math.max(1, query.fromPage())));
        command.add("--max-pages");
        command.add(String.valueOf(Math.max(1, Math.min(query.maxPages(), 20))));

        ProcessResult result = runToCompletion(command);
        return new BiliupCommandResult(maskCommand(command), result.exitCode(), result.output());
    }

    private BiliupJobSnapshot startJob(String commandName) {
        List<String> command = baseCommand();
        command.add(commandName);
        List<String> executableCommand = requiresTerminal(commandName) ? pseudoTerminalCommand(command) : command;
        String id = UUID.randomUUID().toString();
        BiliupJob job = new BiliupJob(id, maskCommand(command), LocalDateTime.now());
        jobs.put(id, job);
        executor.submit(() -> runJob(executableCommand, job));
        return job.snapshot();
    }

    private List<String> baseCommand() {
        List<String> command = new ArrayList<>();
        command.add(binPath.toString());
        if (!proxy.isBlank()) {
            command.add("--proxy");
            command.add(proxy);
        }
        command.add("--user-cookie");
        command.add(cookiePath.toString());
        return command;
    }

    private void runJob(List<String> command, BiliupJob job) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(command)
                    .redirectErrorStream(true)
                    .directory(Path.of(".").toAbsolutePath().normalize().toFile());
            processBuilder.environment().putIfAbsent("TERM", "xterm-256color");
            Process process = processBuilder.start();
            job.setProcess(process);
            job.setInputWriter(new OutputStreamWriter(process.getOutputStream(), StandardCharsets.UTF_8));
            try (BufferedReader reader = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    job.appendOutput(cleanOutput(line));
                    job.appendOutput(System.lineSeparator());
                }
            }
            job.complete(process.waitFor());
        } catch (Exception exception) {
            job.fail(exception);
        }
    }

    private ProcessResult runToCompletion(List<String> command) throws IOException, InterruptedException {
        Process process = new ProcessBuilder(command)
                .redirectErrorStream(true)
                .start();
        StringBuilder output = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                output.append(cleanOutput(line)).append(System.lineSeparator());
            }
        }
        return new ProcessResult(process.waitFor(), output.toString());
    }

    private boolean requiresTerminal(String commandName) {
        return "login".equals(commandName) || "renew".equals(commandName);
    }

    private List<String> pseudoTerminalCommand(List<String> command) {
        List<String> wrapped = new ArrayList<>();
        wrapped.add("/usr/bin/script");
        wrapped.add("-q");
        wrapped.add("/dev/null");
        wrapped.addAll(command);
        return wrapped;
    }

    private String cleanOutput(String line) {
        String withoutAnsi = ANSI_PATTERN.matcher(line).replaceAll("");
        return withoutAnsi.replace("\u0004\b\b", "").stripTrailing();
    }

    private String maskCommand(List<String> command) {
        return String.join(" ", command);
    }

    private record ProcessResult(int exitCode, String output) {
    }
}
