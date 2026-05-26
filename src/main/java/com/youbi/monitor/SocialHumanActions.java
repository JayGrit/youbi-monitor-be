package com.youbi.monitor;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Mouse;
import com.microsoft.playwright.Page;
import com.microsoft.playwright.options.BoundingBox;
import org.springframework.stereotype.Component;

import java.util.concurrent.ThreadLocalRandom;

@Component
public class SocialHumanActions {
    void click(Page page, Locator locator) {
        locator.scrollIntoViewIfNeeded();
        pause(page, 120, 360);
        BoundingBox box = locator.boundingBox();
        if (box == null || box.width <= 0 || box.height <= 0) {
            locator.click();
            return;
        }
        double x = box.x + jitteredOffset(box.width);
        double y = box.y + jitteredOffset(box.height);
        page.mouse().move(x - random(12, 42), y - random(6, 24), new Mouse.MoveOptions().setSteps(randomInt(3, 8)));
        pause(page, 80, 260);
        page.mouse().move(x, y, new Mouse.MoveOptions().setSteps(randomInt(2, 6)));
        pause(page, 40, 160);
        page.mouse().click(x, y);
        pause(page, 120, 420);
    }

    void fill(Page page, Locator locator, String value) {
        click(page, locator);
        page.keyboard().press("Control+A");
        page.keyboard().press("Meta+A");
        page.keyboard().press("Backspace");
        type(page, value);
    }

    void type(Page page, String value) {
        String text = value == null ? "" : value;
        for (int offset = 0; offset < text.length(); ) {
            int end = Math.min(text.length(), offset + randomInt(1, 4));
            page.keyboard().type(text.substring(offset, end), new com.microsoft.playwright.Keyboard.TypeOptions().setDelay(randomInt(20, 95)));
            offset = end;
            if (randomInt(0, 9) == 0) {
                pause(page, 120, 460);
            }
        }
    }

    void pause(Page page, int minMs, int maxMs) {
        page.waitForTimeout(randomInt(minMs, maxMs));
    }

    private double jitteredOffset(double length) {
        double margin = Math.max(4, Math.min(length * 0.22, 18));
        if (length <= margin * 2) {
            return length / 2;
        }
        return random(margin, length - margin);
    }

    private double random(double min, double max) {
        return ThreadLocalRandom.current().nextDouble(min, max);
    }

    private int randomInt(int min, int maxInclusive) {
        return ThreadLocalRandom.current().nextInt(min, maxInclusive + 1);
    }
}
