package com.compagnonsdudev.ui;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Guards the work of removing the dashboard's third-party dependencies.
 *
 * <p>Fonts, JavaScript libraries and the stylesheet are all served by this
 * application. Nothing enforces that at compile time, so a single pasted CDN
 * tag would silently reintroduce an external dependency — and with it the
 * inability to run air-gapped.
 */
class TemplateAssetsTest {

    private static final Path TEMPLATES = Path.of("src/main/resources/templates");

    /** The XML namespace declaration is a URI, not a fetched asset. */
    private static final Pattern EXTERNAL_URL =
            Pattern.compile("(?:src|href)\\s*=\\s*\"(https?://[^\"]+)\"");

    @Test
    void noTemplateFetchesAnAssetFromAnExternalOrigin() throws IOException {
        List<String> offenders = new ArrayList<>();

        try (Stream<Path> templates = Files.walk(TEMPLATES)) {
            for (Path template : templates.filter(p -> p.toString().endsWith(".html")).toList()) {
                Matcher matcher = EXTERNAL_URL.matcher(Files.readString(template));
                while (matcher.find()) {
                    offenders.add(TEMPLATES.relativize(template) + " -> " + matcher.group(1));
                }
            }
        }

        assertTrue(offenders.isEmpty(),
                "Templates must serve their assets locally, see static/vendor/README.md. Found: " + offenders);
    }

    @Test
    void theCompiledStylesheetIsTheOneReferenced() throws IOException {
        String head = Files.readString(TEMPLATES.resolve("fragments/head.html"));

        assertTrue(head.contains("@{/vendor/css/app.css}"),
                "The head fragment must link the stylesheet compiled by frontend-maven-plugin");
        assertTrue(!head.contains("cdn.tailwindcss.com"),
                "Tailwind is compiled at build time and must not be pulled from its CDN");
    }
}
