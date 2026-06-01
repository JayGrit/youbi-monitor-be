package com.youbi.monitor;

import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.io.IOException;
import java.util.List;

import static org.springframework.http.HttpStatus.CONFLICT;
import static org.springframework.http.HttpStatus.INTERNAL_SERVER_ERROR;
import static org.springframework.http.HttpStatus.NOT_FOUND;

@RestController
@CrossOrigin
public class MonitorController {
    private final MonitorService monitorService;
    private final DiagnosticArtifactService diagnosticArtifactService;

    public MonitorController(MonitorService monitorService, DiagnosticArtifactService diagnosticArtifactService) {
        this.monitorService = monitorService;
        this.diagnosticArtifactService = diagnosticArtifactService;
    }

    @GetMapping("/api/video-tasks/monitor")
    public MonitorResponse monitor(@RequestParam(defaultValue = "0") int limit) {
        int boundedLimit = limit <= 0 ? Integer.MAX_VALUE : Math.max(1, limit);
        return monitorService.listTasks(boundedLimit);
    }

    @GetMapping("/api/video-tasks/{taskId}/flow")
    public TaskFlowDetail flow(@PathVariable String taskId) {
        TaskFlowDetail detail = monitorService.getTaskFlow(taskId);
        if (detail == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        return detail;
    }

    @GetMapping("/api/video-tasks/{taskId}/uploader-diagnostics")
    public List<DiagnosticArtifactRecord> uploaderDiagnostics(@PathVariable String taskId) {
        return diagnosticArtifactService.list(taskId, null);
    }

    @PatchMapping("/api/video-tasks/{taskId}/speaker-segments/{segmentId}/dst-text")
    public MonitorService.SpeakerSegmentTextUpdateResult updateSpeakerSegmentDstText(
            @PathVariable String taskId,
            @PathVariable long segmentId,
            @RequestBody SpeakerSegmentTextUpdateRequest request
    ) {
        MonitorService.SpeakerSegmentTextUpdateResult result = monitorService.updateSpeakerSegmentDstText(
                taskId,
                segmentId,
                request == null ? null : request.dstText()
        );
        if (result == null) {
            throw new ResponseStatusException(NOT_FOUND, "Speaker segment does not exist.");
        }
        return result;
    }

    @GetMapping("/api/submitter-author-types")
    public MonitorService.SubmitterAuthorType submitterAuthorType(@RequestParam String author) {
        return monitorService.authorType(author);
    }

    @GetMapping("/api/submitter-author-types/all")
    public List<MonitorService.SubmitterAuthorType> submitterAuthorTypes() {
        return monitorService.authorTypes();
    }

    @PostMapping("/api/submitter-author-types")
    public MonitorService.SubmitterAuthorType saveSubmitterAuthorType(
            @RequestBody MonitorService.SubmitterAuthorTypeUpdateRequest request
    ) {
        try {
            return monitorService.saveAuthorType(
                    request == null ? null : request.author(),
                    request == null ? null : request.type(),
                    request == null ? null : request.needSubtitle(),
                    request == null ? null : request.needDubbing(),
                    request == null ? null : request.sourceLanguage(),
                    request == null ? null : request.targetLanguage()
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @DeleteMapping("/api/submitter-author-types")
    public MonitorService.SubmitterAuthorDeleteResult deleteSubmitterAuthorType(@RequestParam String author) {
        try {
            return monitorService.deleteAuthorType(author);
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/video-tasks/{taskId}/ready")
    public java.util.Map<String, String> markReady(@PathVariable String taskId) {
        if (!monitorService.markTaskReady(taskId)) {
            throw new ResponseStatusException(CONFLICT, "Task has no failed status or does not exist.");
        }
        return java.util.Map.of("status", "ready");
    }

    @GetMapping("/api/upload-submissions/failed")
    public MonitorService.FailedUploadSubmissionList failedUploadSubmissions(@RequestParam String platform) {
        try {
            return monitorService.failedUploadSubmissions(platform);
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/upload-submissions/failed/{platform}/retry")
    public MonitorService.UploadSubmissionRetryResult retryUploadSubmissions(
            @PathVariable String platform,
            @RequestBody MonitorService.UploadSubmissionRetryRequest request
    ) {
        try {
            return monitorService.retryUploadSubmissions(platform, request == null ? List.of() : request.ids());
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @GetMapping("/api/upload-backfill/candidates")
    public MonitorService.UploadBackfillCandidateList uploadBackfillCandidates(
            @RequestParam String platform,
            @RequestParam String accountKey,
            @RequestParam String type
    ) {
        try {
            return monitorService.uploadBackfillCandidates(platform, accountKey, type);
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/upload-backfill/register")
    public MonitorService.UploadBackfillRegisterResult registerUploadBackfill(
            @RequestBody MonitorService.UploadBackfillRegisterRequest request
    ) {
        try {
            return monitorService.registerUploadBackfill(
                    request == null ? null : request.platform(),
                    request == null ? null : request.accountKey(),
                    request == null ? null : request.type(),
                    request == null ? List.of() : request.taskIds()
            );
        } catch (IllegalArgumentException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/video-tasks/{taskId}/restart")
    public MonitorService.TaskRestartResult restart(@PathVariable String taskId) {
        try {
            MonitorService.TaskRestartResult result = monitorService.restartTask(taskId);
            if (result == null) {
                throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
            }
            return result;
        } catch (IllegalStateException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        } catch (IOException exc) {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, exc.getMessage(), exc);
        }
    }

    @PostMapping("/api/video-tasks/{taskId}/stop")
    public MonitorService.TaskStopResult stop(@PathVariable String taskId) {
        MonitorService.TaskStopResult result = monitorService.stopTask(taskId);
        if (result == null) {
            throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
        }
        if (!result.stoppedTask()) {
            throw new ResponseStatusException(CONFLICT, "Task is not running.");
        }
        return result;
    }

    @DeleteMapping("/api/video-tasks/{taskId}")
    public MonitorService.TaskDeleteResult delete(@PathVariable String taskId) {
        try {
            MonitorService.TaskDeleteResult result = monitorService.deleteTask(taskId);
            if (result == null) {
                throw new ResponseStatusException(NOT_FOUND, "Task does not exist.");
            }
            return result;
        } catch (IllegalStateException exc) {
            throw new ResponseStatusException(CONFLICT, exc.getMessage(), exc);
        } catch (IOException exc) {
            throw new ResponseStatusException(INTERNAL_SERVER_ERROR, exc.getMessage(), exc);
        }
    }

    @GetMapping("/health")
    public java.util.Map<String, String> health() {
        return java.util.Map.of("status", "ok");
    }
}
