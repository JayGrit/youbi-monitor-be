package com.youbi.monitor.service;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.imageio.ImageIO;
import java.awt.Color;
import java.awt.image.BufferedImage;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;

class JinritoutiaoUploadServiceTest {

    @TempDir
    Path tempDir;

    @Test
    void registersWebpImageReader() {
        assertThat(ImageIO.getImageReadersByMIMEType("image/webp").hasNext()).isTrue();
    }

    @Test
    void normalizesCoverToLargeJpeg() throws Exception {
        Path source = tempDir.resolve("cover.png");
        BufferedImage image = new BufferedImage(480, 360, BufferedImage.TYPE_INT_ARGB);
        image.setRGB(0, 0, Color.RED.getRGB());
        assertThat(ImageIO.write(image, "png", source.toFile())).isTrue();

        Path prepared = JinritoutiaoUploadService.prepareCoverForUpload(source, "test-task");
        try {
            BufferedImage result = ImageIO.read(prepared.toFile());
            assertThat(prepared).isNotEqualTo(source);
            assertThat(prepared.getFileName().toString()).endsWith(".jpg");
            assertThat(Files.size(prepared)).isPositive();
            assertThat(result.getWidth()).isGreaterThanOrEqualTo(1920);
            assertThat(result.getHeight()).isGreaterThanOrEqualTo(1080);
            assertThat(result.getType()).isEqualTo(BufferedImage.TYPE_3BYTE_BGR);
        } finally {
            Files.deleteIfExists(prepared);
        }
    }

    @Test
    void recognizesNewPublishWarning() {
        assertThat(JinritoutiaoUploadService.isPublishConfirmation(
                "温馨提示\n视频标题涉嫌夸张，继续发布可能会影响推荐效果\n返回修改\n继续发表"
        )).isTrue();
    }

    @Test
    void recognizesLegacyPublishWarningButton() {
        assertThat(JinritoutiaoUploadService.isPublishConfirmation(
                "温馨提示\n标题涉嫌夸张\n返回修改\n继续发布"
        )).isTrue();
    }

    @Test
    void ignoresUnrelatedDialog() {
        assertThat(JinritoutiaoUploadService.isPublishConfirmation(
                "温馨提示\n账号登录已失效\n重新登录"
        )).isFalse();
    }

    @Test
    void classifiesPublishRateLimitMessage() {
        assertThat(JinritoutiaoUploadService.submitPlatformError(
                "发布视频\n发文频繁，先休息一下吧"
        )).isEqualTo("今日头条发布频控（RATE_LIMITED）：发文频繁，先休息一下吧");
    }

    @Test
    void ignoresNormalPublishPageText() {
        assertThat(JinritoutiaoUploadService.submitPlatformError(
                "上传成功\n存草稿\n定时发布\n发布"
        )).isNull();
    }
}
