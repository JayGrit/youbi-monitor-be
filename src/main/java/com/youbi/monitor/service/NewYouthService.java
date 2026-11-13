package com.youbi.monitor.service;

import com.youbi.monitor.repository.MonitorRepository;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@Service
public class NewYouthService {
    private static final ZoneId SHANGHAI = ZoneId.of("Asia/Shanghai");
    private final MonitorRepository repository;

    public NewYouthService(MonitorRepository repository) {
        this.repository = repository;
    }

    public Map<String, Object> overview() {
        LocalDateTime now = LocalDateTime.now(SHANGHAI);
        LocalDateTime hour = now.withMinute(0).withSecond(0).withNano(0);
        LocalDateTime midnight = now.toLocalDate().atStartOfDay();
        Long accountId = repository.query("""
                SELECT id FROM uploader_account
                WHERE platform = 'bilibili' AND topic = 'newyouth' AND bilibili_uid = 678811807
                LIMIT 1
                """, (rs, ignored) -> rs.getLong(1)).stream().findFirst().orElse(null);

        Map<String, Object> response = new LinkedHashMap<>();
        response.put("generatedAt", now.toString());
        response.put("accountId", accountId);
        response.put("uid", "678811807");
        if (accountId == null) {
            response.put("previousHour", emptyPie(hour.minusHours(1), hour));
            response.put("today", emptyPie(midnight, hour));
            response.put("followers", List.of());
            return response;
        }

        Map<LocalDateTime, Run> runs = new HashMap<>();
        repository.query("""
                SELECT id, scheduled_slot FROM product_podcast_web_run
                WHERE uploader_account_id = ? AND finished_at IS NOT NULL
                  AND scheduled_slot BETWEEN ? AND ?
                ORDER BY scheduled_slot
                """, (rs, ignored) -> new Run(rs.getLong("id"), rs.getTimestamp("scheduled_slot").toLocalDateTime()),
                accountId, Timestamp.valueOf(midnight.minusHours(1)), Timestamp.valueOf(hour))
                .forEach(run -> runs.put(run.slot(), run));

        response.put("previousHour", pie(runs.get(hour.minusHours(1)), runs.get(hour),
                hour.minusHours(1), hour, accountId));
        Run baseline = runs.get(midnight);
        Run latest = runs.values().stream()
                .filter(run -> !run.slot().isBefore(midnight))
                .max(Comparator.comparing(Run::slot)).orElse(null);
        response.put("today", pie(baseline, latest, midnight,
                latest == null ? hour : latest.slot(), accountId));
        response.put("followers", followerHours(runs, midnight, hour, accountId));
        return response;
    }

    private Map<String, Object> pie(Run start, Run end, LocalDateTime startSlot,
                                    LocalDateTime endSlot, long accountId) {
        if (start == null || end == null || !end.slot().isAfter(start.slot())) {
            return emptyPie(startSlot, endSlot);
        }
        List<VideoGrowth> rows = repository.query("""
                SELECT current_data.task_id, current_data.bvid,
                       COALESCE(NULLIF(inventory.title, ''), NULLIF(podcast.title, ''), current_data.bvid) AS title,
                       COALESCE(previous_data.view_count, 0) AS previous_views,
                       current_data.view_count AS current_views
                FROM (
                    SELECT bvid, MAX(run_id) AS run_id
                    FROM product_podcast_web_video_data_history
                    WHERE uploader_account_id = ? AND run_id <= ? AND view_count IS NOT NULL
                    GROUP BY bvid
                ) current_latest
                JOIN product_podcast_web_video_data_history current_data
                    ON current_data.bvid = current_latest.bvid
                    AND current_data.run_id = current_latest.run_id
                LEFT JOIN (
                    SELECT bvid, MAX(run_id) AS run_id
                    FROM product_podcast_web_video_data_history
                    WHERE uploader_account_id = ? AND run_id <= ? AND view_count IS NOT NULL
                    GROUP BY bvid
                ) previous_latest ON previous_latest.bvid = current_data.bvid
                LEFT JOIN product_podcast_web_video_data_history previous_data
                    ON previous_data.bvid = previous_latest.bvid
                    AND previous_data.run_id = previous_latest.run_id
                LEFT JOIN product_podcast podcast ON podcast.task_id = current_data.task_id
                LEFT JOIN product_podcast_web_video_inventory inventory
                    ON inventory.uploader_account_id = ? AND inventory.bvid = current_data.bvid
                """, (rs, ignored) -> new VideoGrowth(rs.getString("task_id"), rs.getString("bvid"),
                cleanTitle(rs.getString("title")), rs.getLong("previous_views"), rs.getLong("current_views")),
                accountId, end.id(), accountId, start.id(), accountId);
        return buildPie(startSlot, endSlot, rows);
    }

    static Map<String, Object> buildPie(LocalDateTime start, LocalDateTime end, List<VideoGrowth> rows) {
        List<VideoGrowth> positives = rows.stream().filter(row -> row.growth() > 0)
                .sorted(Comparator.comparingLong(VideoGrowth::growth).reversed()
                        .thenComparing(VideoGrowth::bvid)).toList();
        long total = positives.stream().mapToLong(VideoGrowth::growth).sum();
        long topTotal = positives.stream().limit(10).mapToLong(VideoGrowth::growth).sum();
        List<Map<String, Object>> slices = new ArrayList<>();
        positives.stream().limit(10).forEach(row -> {
            Map<String, Object> slice = new LinkedHashMap<>();
            slice.put("taskId", row.taskId());
            slice.put("bvid", row.bvid());
            slice.put("title", row.title());
            slice.put("url", "https://www.bilibili.com/video/" + row.bvid());
            slice.put("growth", row.growth());
            slice.put("previousViews", row.previousViews());
            slice.put("currentViews", row.currentViews());
            slices.add(slice);
        });
        if (positives.size() > 10) {
            slices.add(Map.of("title", "其他", "growth", total - topTotal));
        }
        Map<String, Object> result = emptyPie(start, end);
        result.put("available", true);
        result.put("totalGrowth", total);
        result.put("topTenGrowth", topTotal);
        result.put("otherGrowth", total - topTotal);
        result.put("comparableVideos", rows.size());
        result.put("negativeCorrections", rows.stream().filter(row -> row.growth() < 0).count());
        result.put("slices", slices);
        return result;
    }

    private static Map<String, Object> emptyPie(LocalDateTime start, LocalDateTime end) {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("available", false);
        result.put("startSlot", start.toString());
        result.put("endSlot", end.toString());
        result.put("totalGrowth", 0L);
        result.put("topTenGrowth", 0L);
        result.put("otherGrowth", 0L);
        result.put("comparableVideos", 0);
        result.put("negativeCorrections", 0);
        result.put("slices", List.of());
        return result;
    }

    private List<Map<String, Object>> followerHours(Map<LocalDateTime, Run> runs,
                                                    LocalDateTime midnight, LocalDateTime hour,
                                                    long accountId) {
        Map<Long, Long> counts = new HashMap<>();
        repository.query("""
                SELECT history.run_id, history.follower_count
                FROM product_podcast_web_account_data_history history
                JOIN product_podcast_web_run run ON run.id = history.run_id
                WHERE history.uploader_account_id = ? AND history.follower_count IS NOT NULL
                  AND run.finished_at IS NOT NULL
                  AND run.scheduled_slot BETWEEN ? AND ?
                """, (rs, ignored) -> Map.entry(rs.getLong("run_id"), rs.getLong("follower_count")),
                accountId, Timestamp.valueOf(midnight.minusHours(1)), Timestamp.valueOf(hour))
                .forEach(entry -> counts.put(entry.getKey(), entry.getValue()));
        List<Map<String, Object>> points = new ArrayList<>();
        for (LocalDateTime slot = midnight; !slot.isAfter(hour); slot = slot.plusHours(1)) {
            Run current = runs.get(slot);
            Run previous = runs.get(slot.minusHours(1));
            Long currentCount = current == null ? null : counts.get(current.id());
            Long previousCount = previous == null ? null : counts.get(previous.id());
            Map<String, Object> point = new LinkedHashMap<>();
            point.put("slot", slot.toString());
            point.put("hour", String.format("%02d:00", slot.getHour()));
            point.put("followers", currentCount);
            point.put("previousFollowers", previousCount);
            point.put("growth", currentCount == null || previousCount == null ? null : currentCount - previousCount);
            points.add(point);
        }
        return points;
    }

    private static String cleanTitle(String title) {
        return title == null ? "" : title.replace("\\n", " ").replace('\n', ' ').replace('\r', ' ').trim();
    }

    record Run(long id, LocalDateTime slot) {}

    record VideoGrowth(String taskId, String bvid, String title, long previousViews, long currentViews) {
        long growth() { return currentViews - previousViews; }
    }
}
