package com.youbi.monitor.dto;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ShipinhaoUploadRequestTest {

    @Test
    void normalizesExplicitShortTitleWhenRequestIsCreated() {
        ShipinhaoUploadRequest request = request(
                "ASMR美食制作，超解压！#推荐@热门",
                "猫,狗：好？100%℃《美食》"
        );

        assertEquals("猫 狗：好？100%℃《美食》", request.shortTitle());
    }

    @Test
    void fallsBackToTitleAndPadsToAtLeastSixCharacters() {
        ShipinhaoUploadRequest request = request("【中配】猫！", null);

        assertEquals("猫视频视频视频", request.shortTitle());
    }

    @Test
    void usesFallbackWhenNoSupportedCharactersRemain() {
        ShipinhaoUploadRequest request = request("!!!", "🚀");

        assertEquals("精彩视频视频", request.shortTitle());
    }

    @Test
    void truncatesShortTitleToSixteenCharacters() {
        ShipinhaoUploadRequest request = request(
                "备用标题",
                "一二三四五六七八九十一二三四五六七八九十"
        );

        assertEquals("一二三四五六七八九十一二三四五六", request.shortTitle());
    }

    private ShipinhaoUploadRequest request(String title, String shortTitle) {
        return new ShipinhaoUploadRequest(
                "default",
                "task-id",
                null,
                "https://example.com/video.mp4",
                null,
                null,
                null,
                null,
                title,
                null,
                null,
                null,
                null,
                shortTitle,
                false
        );
    }
}
