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
}
