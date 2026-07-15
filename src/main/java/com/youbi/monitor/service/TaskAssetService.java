package com.youbi.monitor.service;

import com.youbi.monitor.model.TaskFlowDetail;
import io.minio.ListObjectsArgs;
import io.minio.MinioClient;
import io.minio.Result;
import io.minio.messages.Item;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;

@Service
public class TaskAssetService {
    private final MinioClient minioClient;
    private final String minioEndpoint;
    private final String minioBucket;

    public TaskAssetService(
            @Value("${youbi.minio.endpoint}") String minioEndpoint,
            @Value("${youbi.minio.access-key}") String minioAccessKey,
            @Value("${youbi.minio.secret-key}") String minioSecretKey,
            @Value("${youbi.minio.bucket}") String minioBucket
    ) {
        this.minioEndpoint = text(minioEndpoint);
        this.minioBucket = text(minioBucket).isBlank() ? "ydbi" : text(minioBucket);
        this.minioClient = MinioClient.builder()
                .endpoint(minioEndpoint)
                .credentials(minioAccessKey, minioSecretKey)
                .build();
    }

    public List<TaskFlowDetail.TaskFlowAsset> listTaskAssets(String taskId) {
        List<TaskFlowDetail.TaskFlowAsset> assets = new ArrayList<>();
        String prefix = minioPrefix(taskId);
        Iterable<Result<Item>> objects = minioClient.listObjects(
                ListObjectsArgs.builder()
                        .bucket(minioBucket)
                        .prefix(prefix)
                        .recursive(true)
                        .build()
        );
        try {
            for (Result<Item> result : objects) {
                Item item = result.get();
                assets.add(new TaskFlowDetail.TaskFlowAsset(
                        objectDisplayName(item.objectName()),
                        stageFromObject(item.objectName()),
                        kindForName(item.objectName()),
                        publicObjectUrl(item.objectName()),
                        item.objectName(),
                        item.size(),
                        item.lastModified() == null ? null : LocalDateTime.ofInstant(item.lastModified().toInstant(), ZoneId.systemDefault())
                ));
            }
        } catch (Exception exc) {
            assets.add(new TaskFlowDetail.TaskFlowAsset(
                    "MinIO 列表失败",
                    "",
                    "error",
                    "",
                    prefix,
                    null,
                    null
            ));
        }
        return assets;
    }

    public TaskFlowDetail.TaskFlowAsset assetFor(
            String fieldName,
            String stageKey,
            Object value,
            List<TaskFlowDetail.TaskFlowAsset> minioObjects
    ) {
        String text = stringValue(value);
        if (text.isBlank()) {
            return null;
        }
        String objectName = objectNameFromRef(text);
        if (objectName != null) {
            for (TaskFlowDetail.TaskFlowAsset asset : minioObjects) {
                if (objectName.equals(asset.objectName())) {
                    return new TaskFlowDetail.TaskFlowAsset(
                            fieldName,
                            stageKey,
                            kindForField(fieldName, asset.url()),
                            text.startsWith("http://") || text.startsWith("https://") ? text : asset.url(),
                            objectName,
                            asset.size(),
                            asset.lastModified()
                    );
                }
            }
            String url = text.startsWith("http://") || text.startsWith("https://") ? text : publicObjectUrl(objectName);
            return new TaskFlowDetail.TaskFlowAsset(fieldName, stageKey, kindForField(fieldName, url), url, objectName, null, null);
        }
        if (text.startsWith("http://") || text.startsWith("https://")) {
            return new TaskFlowDetail.TaskFlowAsset(fieldName, stageKey, kindForField(fieldName, text), text, null, null, null);
        }
        return null;
    }

    private String publicObjectUrl(String objectName) {
        String endpoint = minioEndpoint.replaceFirst("/+$", "");
        return endpoint + "/" + minioBucket + "/" + objectName.replaceFirst("^/+", "");
    }

    private String objectNameFromRef(String ref) {
        String value = text(ref);
        if (value.isBlank() || value.startsWith("db://")) {
            return null;
        }
        if (value.startsWith("http://")) {
            try {
                URI uri = URI.create(value);
                String prefix = "/" + minioBucket + "/";
                if ("120.53.92.66".equals(uri.getHost()) && uri.getPort() == 9000 && uri.getPath().startsWith(prefix)) {
                    return uri.getPath().substring(prefix.length());
                }
            } catch (IllegalArgumentException ignored) {
                return null;
            }
        }
        return null;
    }

    private static String stageFromObject(String objectName) {
        String[] parts = objectName.split("/");
        return parts.length >= 2 ? parts[1] : "";
    }

    private static String objectDisplayName(String objectName) {
        int slash = objectName.lastIndexOf('/');
        return slash >= 0 ? objectName.substring(slash + 1) : objectName;
    }

    private static String kindForName(String name) {
        String lower = text(name).toLowerCase();
        if (lower.matches(".*\\.(mp4|mov|m4v|webm)$")) {
            return "video";
        }
        if (lower.matches(".*\\.(wav|mp3|m4a|aac|flac|ogg|webm)$")) {
            return "audio";
        }
        if (lower.matches(".*\\.(png|jpg|jpeg|webp|gif)$")) {
            return "image";
        }
        if (lower.matches(".*\\.(json)$") || lower.startsWith("db://")) {
            return "json";
        }
        if (lower.endsWith(".srt") || lower.endsWith(".vtt") || lower.endsWith(".txt")) {
            return "text";
        }
        return "file";
    }

    private static String kindForField(String fieldName, String name) {
        String lowerField = text(fieldName).toLowerCase();
        if (lowerField.contains("audio") || lowerField.contains("wav")) {
            return "audio";
        }
        if (lowerField.contains("video")) {
            return "video";
        }
        if (lowerField.contains("thumbnail") || lowerField.contains("cover")) {
            return "image";
        }
        return kindForName(name);
    }

    private static String minioPrefix(String taskId) {
        String clean = text(taskId).replaceFirst("^/+", "").replaceFirst("/+$", "");
        if (clean.isBlank()) {
            throw new IllegalArgumentException("Missing taskId");
        }
        return clean + "/";
    }

    private static String stringValue(Object value) {
        return value == null ? "" : String.valueOf(value).trim();
    }

    private static String text(String value) {
        return value == null ? "" : value.trim();
    }
}
