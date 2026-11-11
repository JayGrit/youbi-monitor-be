package com.youbi.monitor.service;

import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class NewYouthServiceTest {
    @Test
    void pieKeepsTenLargestGainsAndGroupsTheRest() {
        List<NewYouthService.VideoGrowth> rows = new ArrayList<>();
        for (int gain = 1; gain <= 12; gain++) {
            rows.add(new NewYouthService.VideoGrowth("task-" + gain, "BV" + gain,
                    "Video " + gain, 100, 100 + gain));
        }
        rows.add(new NewYouthService.VideoGrowth("corrected", "BV0", "Corrected", 120, 110));

        Map<String, Object> pie = NewYouthService.buildPie(
                LocalDateTime.of(2026, 11, 12, 1, 0),
                LocalDateTime.of(2026, 11, 12, 2, 0), rows);

        assertEquals(true, pie.get("available"));
        assertEquals(78L, pie.get("totalGrowth"));
        assertEquals(75L, pie.get("topTenGrowth"));
        assertEquals(3L, pie.get("otherGrowth"));
        assertEquals(13, pie.get("comparableVideos"));
        assertEquals(1L, pie.get("negativeCorrections"));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> slices = (List<Map<String, Object>>) pie.get("slices");
        assertEquals(11, slices.size());
        assertEquals(12L, slices.get(0).get("growth"));
        assertEquals("其他", slices.get(10).get("title"));
    }
}
