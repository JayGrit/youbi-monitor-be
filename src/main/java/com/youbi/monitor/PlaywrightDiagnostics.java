package com.youbi.monitor;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;
import org.slf4j.Logger;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

final class PlaywrightDiagnostics {
    private PlaywrightDiagnostics() {
    }

    static DiagnosticSnapshot dump(Page page, Path diagnosticsRoot, String taskId, String label, Logger log, String logPrefix, boolean failOnError) {
        try {
            Path dir = diagnosticsRoot.resolve(TextSupport.safeSegment(taskId));
            Files.createDirectories(dir);
            Path screenshot = dir.resolve(label + ".png");
            Path html = dir.resolve(label + ".html");
            page.screenshot(new Page.ScreenshotOptions().setPath(screenshot).setFullPage(true).setTimeout(10000));
            Files.writeString(html, page.content(), StandardCharsets.UTF_8);
            log.info("{} diagnostics dumped taskId={} label={} screenshot={} html={}", logPrefix, taskId, label, screenshot, html);
            return new DiagnosticSnapshot(screenshot, html);
        } catch (Exception exception) {
            log.warn("{} diagnostics dump failed taskId={} label={} message={}", logPrefix, taskId, label, exception.getMessage());
            if (failOnError) {
                throw new RuntimeException("Cannot dump " + logPrefix + " diagnostics: " + exception.getMessage(), exception);
            }
            return new DiagnosticSnapshot(null, null);
        }
    }

    static String safeBodyText(Page page) {
        try {
            return page.locator("body").innerText(new Locator.InnerTextOptions().setTimeout(3000));
        } catch (Exception exception) {
            return "";
        }
    }

    static String visibleButtonTexts(Page page) {
        try {
            return String.join(" | ", page.locator("button:visible").allTextContents().stream()
                    .map(TextSupport::text)
                    .filter(value -> !value.isBlank())
                    .toList());
        } catch (Exception exception) {
            return "cannot-read-buttons: " + exception.getMessage();
        }
    }

    static int exactTextMatchCount(Page page, String buttonText) {
        try {
            return page.locator("text=\"" + buttonText + "\"").count();
        } catch (Exception ignored) {
            return 0;
        }
    }

    static String exactTextElements(Page page, String buttonText) {
        try {
            return page.locator("text=\"" + buttonText + "\"").evaluateAll(
                    """
                    (els) => els.map((el, i) => {
                      const rect = el.getBoundingClientRect();
                      const style = window.getComputedStyle(el);
                      return `${i}:${el.tagName}.${el.className || ''}:visible=${!!(rect.width && rect.height) && style.visibility !== 'hidden' && style.display !== 'none'}:rect=${Math.round(rect.x)},${Math.round(rect.y)},${Math.round(rect.width)},${Math.round(rect.height)}:text=${(el.innerText || el.textContent || '').trim()}`;
                    }).join(' | ')
                    """
            ).toString();
        } catch (Exception exception) {
            return "cannot-read-exact-text: " + exception.getMessage();
        }
    }

    record DiagnosticSnapshot(Path screenshot, Path html) {
    }
}
