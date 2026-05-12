package com.youbi.monitor;

import java.io.IOException;
import java.io.Writer;
import java.time.LocalDateTime;

public class BiliupJob {
    private final String id;
    private final String command;
    private final LocalDateTime startedAt;
    private final StringBuilder output = new StringBuilder();
    private volatile String status = "running";
    private volatile Integer exitCode;
    private volatile LocalDateTime completedAt;
    private volatile Writer inputWriter;
    private volatile Process process;

    public BiliupJob(String id, String command, LocalDateTime startedAt) {
        this.id = id;
        this.command = command;
        this.startedAt = startedAt;
    }

    public synchronized void appendOutput(String text) {
        output.append(text);
    }

    public synchronized BiliupJobSnapshot snapshot() {
        return new BiliupJobSnapshot(id, command, status, exitCode, startedAt, completedAt, output.toString());
    }

    public void setInputWriter(Writer inputWriter) {
        this.inputWriter = inputWriter;
    }

    public void setProcess(Process process) {
        this.process = process;
    }

    public synchronized void sendInput(String input) throws IOException {
        if (!"running".equals(status)) {
            throw new IllegalStateException("Biliup job is not running.");
        }
        if (inputWriter == null) {
            throw new IllegalStateException("Biliup job is not ready for input.");
        }
        appendOutput("[monitor] sent input: ");
        appendOutput(describeInput(input));
        appendOutput(System.lineSeparator());
        inputWriter.write(input);
        inputWriter.flush();
    }

    private String describeInput(String input) {
        return switch (input) {
            case "\u001B[A" -> "up";
            case "\u001B[B" -> "down";
            case "\n" -> "enter";
            default -> input == null || input.isBlank() ? "blank" : "text";
        };
    }

    public void complete(int exitCode) {
        this.exitCode = exitCode;
        this.status = exitCode == 0 ? "success" : "failed";
        this.completedAt = LocalDateTime.now();
        closeInputWriter();
    }

    public void fail(Exception exception) {
        appendOutput(exception.getMessage() == null ? exception.toString() : exception.getMessage());
        appendOutput(System.lineSeparator());
        this.status = "failed";
        this.completedAt = LocalDateTime.now();
        closeInputWriter();
    }

    public synchronized void cancel() {
        if (!"running".equals(status)) {
            return;
        }
        Process runningProcess = process;
        if (runningProcess != null) {
            runningProcess.destroy();
        }
        this.status = "cancelled";
        this.completedAt = LocalDateTime.now();
        appendOutput("Cancelled by monitor.");
        appendOutput(System.lineSeparator());
        closeInputWriter();
    }

    private void closeInputWriter() {
        Writer writer = inputWriter;
        inputWriter = null;
        if (writer == null) {
            return;
        }
        try {
            writer.close();
        } catch (IOException ignored) {
        }
    }
}
