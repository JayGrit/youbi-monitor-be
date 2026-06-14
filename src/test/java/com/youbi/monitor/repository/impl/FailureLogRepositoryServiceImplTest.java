package com.youbi.monitor.repository.impl;

import com.youbi.monitor.model.FailureLogActualPublishedResult;
import com.youbi.monitor.repository.MonitorRepository;
import com.youbi.monitor.repository.RowMapper;
import org.junit.jupiter.api.Test;
import org.mockito.invocation.InvocationOnMock;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class FailureLogRepositoryServiceImplTest {

    @Test
    void actualPublishedCompletesParentsWhenItIsTheLastPlatform() throws Exception {
        TestContext context = context(List.of("success", "no_need", "no_need", "no_need", "no_need", "success"));

        FailureLogActualPublishedResult result = context.service().markActualPublished("uploader:jinritoutiao:525");

        assertThat(result.submissionStatus()).isEqualTo("success");
        assertThat(result.uploaderStatus()).isEqualTo("success");
        assertThat(result.taskStatus()).isEqualTo("success");
        assertParentUpdate(context.updates(), "success", "done");
    }

    @Test
    void actualPublishedKeepsParentsFailedWhenAnotherPlatformFailed() throws Exception {
        TestContext context = context(List.of("success", "failed", "no_need", "no_need", "no_need", "success"));

        FailureLogActualPublishedResult result = context.service().markActualPublished("uploader:jinritoutiao:525");

        assertThat(result.submissionStatus()).isEqualTo("success");
        assertThat(result.uploaderStatus()).isEqualTo("failed");
        assertThat(result.taskStatus()).isEqualTo("failed");
        assertParentUpdate(context.updates(), "failed", "uploader");
    }

    private static TestContext context(List<String> platformStatuses) throws Exception {
        MonitorRepository repository = mock(MonitorRepository.class);
        List<SqlUpdate> updates = new ArrayList<>();

        when(repository.queryForObject(anyString(), eq(Integer.class), any(Object[].class))).thenReturn(0);
        when(repository.queryForObject(anyString(), any(RowMapper.class), any(Object[].class)))
                .thenAnswer(invocation -> queryRow(invocation, platformStatuses));
        when(repository.update(anyString(), any(Object[].class))).thenAnswer(invocation -> {
            Object[] arguments = invocation.getArguments();
            updates.add(new SqlUpdate(
                    invocation.getArgument(0),
                    Arrays.copyOfRange(arguments, 1, arguments.length)
            ));
            return 1;
        });

        return new TestContext(new FailureLogRepositoryServiceImpl(repository), updates);
    }

    @SuppressWarnings("unchecked")
    private static Object queryRow(InvocationOnMock invocation, List<String> platformStatuses) throws Exception {
        String sql = invocation.getArgument(0);
        RowMapper<Object> mapper = invocation.getArgument(1);
        ResultSet resultSet = mock(ResultSet.class);
        if (sql.contains("FROM `uploader_task_jinritoutiao`")) {
            when(resultSet.getLong("id")).thenReturn(525L);
            when(resultSet.getString("task_id")).thenReturn("Qf0ss86m0dY");
            when(resultSet.getString("account_key")).thenReturn("long_knowledge");
            when(resultSet.getString("status")).thenReturn("failed");
        } else if (sql.contains("FROM uploader")) {
            when(resultSet.getString("bilibili")).thenReturn(platformStatuses.get(0));
            when(resultSet.getString("douyin")).thenReturn(platformStatuses.get(1));
            when(resultSet.getString("xiaohongshu")).thenReturn(platformStatuses.get(2));
            when(resultSet.getString("shipinhao")).thenReturn(platformStatuses.get(3));
            when(resultSet.getString("kuaishou")).thenReturn(platformStatuses.get(4));
            when(resultSet.getString("jinritoutiao")).thenReturn(platformStatuses.get(5));
        }
        return mapper.mapRow(resultSet, 0);
    }

    private static void assertParentUpdate(List<SqlUpdate> updates, String expectedStatus, String expectedStage) {
        SqlUpdate uploaderUpdate = updates.stream()
                .filter(update -> normalizedSql(update.sql()).contains("UPDATE uploader SET status = ?"))
                .findFirst()
                .orElseThrow();
        assertThat(uploaderUpdate.args()[0]).isEqualTo(expectedStatus);

        SqlUpdate taskUpdate = updates.stream()
                .filter(update -> normalizedSql(update.sql()).contains("UPDATE task SET status = ?"))
                .findFirst()
                .orElseThrow();
        assertThat(taskUpdate.args()[0]).isEqualTo(expectedStatus);
        assertThat(taskUpdate.args()[1]).isEqualTo(expectedStage);
    }

    private static String normalizedSql(String sql) {
        return sql.replaceAll("\\s+", " ").trim();
    }

    private record TestContext(FailureLogRepositoryServiceImpl service, List<SqlUpdate> updates) {
    }

    private record SqlUpdate(String sql, Object[] args) {
    }
}
