package com.youbi.monitor.service;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NewYouthServiceTest {
    @Test
    void pieListsEveryGainAndAggregatesByPublishDate() {
        List<NewYouthService.VideoGrowth> rows = new ArrayList<>();
        for (int gain = 1; gain <= 12; gain++) {
            rows.add(new NewYouthService.VideoGrowth("task-" + gain, "BV" + gain,
                    "Video " + gain, 100, 100 + gain,
                    LocalDateTime.of(2026, 11, gain <= 4 ? 13 : gain <= 8 ? 11 : 8, 12, 0)));
        }
        rows.add(new NewYouthService.VideoGrowth("corrected", "BV0", "Corrected", 120, 110,
                LocalDateTime.of(2026, 11, 12, 12, 0)));

        Map<String, Object> pie = NewYouthService.buildPie(
                LocalDateTime.of(2026, 11, 12, 1, 0),
                LocalDateTime.of(2026, 11, 13, 2, 0), rows);

        assertEquals(true, pie.get("available"));
        assertEquals(78L, pie.get("totalGrowth"));
        assertEquals(13, pie.get("comparableVideos"));
        assertEquals(1L, pie.get("negativeCorrections"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> slices = (List<Map<String, Object>>) pie.get("slices");
        assertEquals(12, slices.size());
        assertEquals(12L, slices.get(0).get("growth"));
        assertEquals(1L, slices.get(11).get("growth"));
        assertEquals("today", slices.get(11).get("ageGroup"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> groups = (List<Map<String, Object>>) pie.get("groups");
        assertEquals(List.of(10L, 26L, 42L), groups.stream().map(group -> group.get("growth")).toList());
    }
    @Test
    void pieIncludesUnassociatedBv() {
        var rows = List.of(new NewYouthService.VideoGrowth(null, "BVunassociated",
                "Unpaired video", 10, 15, null));
        Map<String, Object> pie = NewYouthService.buildPie(
                LocalDateTime.of(2026, 11, 12, 1, 0),
                LocalDateTime.of(2026, 11, 12, 2, 0), rows);
        assertEquals(5L, pie.get("totalGrowth"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> slices = (List<Map<String, Object>>) pie.get("slices");
        assertEquals("BVunassociated", slices.get(0).get("bvid"));
        assertEquals("earlier", slices.get(0).get("ageGroup"));
    }

}
