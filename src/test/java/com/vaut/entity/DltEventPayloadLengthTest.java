package com.vaut.entity;

import com.vaut.repository.DltEventRepository;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ActiveProfiles;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks that the DLT payload and error message survive a round trip well beyond 4000 characters.
 *
 * <p>This does <b>not</b> reproduce the original defect. That was a mismatch between the entity,
 * which declared {@code length = 10000}, and the hand-written Oracle DDL in {@code init.sql}, which
 * created VARCHAR2(4000); Hibernate does not validate column lengths, so it only surfaced when a
 * real message exceeded 4000 characters. Under the dev and test profiles the schema is generated
 * from the entity itself, so no mismatch can exist here and this test passes against the old
 * mapping too.</p>
 *
 * <p>What it does guard is that the {@code @Lob} mapping stores and reads back long values intact,
 * and that nobody narrows these columns later. Catching entity-versus-{@code init.sql} drift needs
 * a test running the real DDL against Oracle with {@code ddl-auto=validate}; the Testcontainers
 * oracle-xe dependency is already declared but no test uses it yet.</p>
 */
@DataJpaTest
@ActiveProfiles("dev")
class DltEventPayloadLengthTest {

    @Autowired
    private DltEventRepository dltEventRepository;

    @Test
    void storesPayloadAndErrorMessageBeyondTheOldVarcharLimit() {
        String longPayload = "{\"data\":\"" + "x".repeat(9000) + "\"}";
        String longErrorMessage = "Persistence failed: " + "y".repeat(9000);

        DltEvent saved = dltEventRepository.save(DltEvent.builder()
                .originalTopic("topic")
                .originalPartition(0)
                .originalOffset(1L)
                .originalKey("key-1")
                .payload(longPayload)
                .errorMessage(longErrorMessage)
                .dhm(LocalDateTime.now())
                .status("UNRESOLVED")
                .severity("MEDIUM")
                .build());
        dltEventRepository.flush();

        DltEvent reloaded = dltEventRepository.findById(saved.getId()).orElseThrow();
        assertThat(reloaded.getPayload()).isEqualTo(longPayload);
        assertThat(reloaded.getErrorMessage()).isEqualTo(longErrorMessage);
    }
}
