package com.youbi.monitor.service;

import com.microsoft.playwright.Locator;
import com.microsoft.playwright.Page;

final class PlaywrightDiagnostics {
    private PlaywrightDiagnostics() {
    }

    static String safeBodyText(Page page) {
        try {
            return page.locator("body").innerText(new Locator.InnerTextOptions().setTimeout(3000));
        } catch (Exception exception) {
            try {
                Object text = page.evaluate("() => document.body ? (document.body.innerText || document.body.textContent || '') : ''");
                return text == null ? "" : text.toString();
            } catch (Exception ignored) {
                return "";
            }
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
}
