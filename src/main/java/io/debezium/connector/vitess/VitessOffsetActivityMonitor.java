/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Duration;
import java.util.Objects;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;
import io.debezium.pipeline.monitor.StaleOffsetsResult;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The restart and current VGTIDs are compared against the values captured when the monitor
 * was last consulted, and when neither has moved, a stale result is reported. The restart
 * VGTID is the position persisted in the offsets and therefore governs where streaming
 * resumes; it only advances once a transaction has been fully processed, so the current VGTID
 * is additionally compared so that progress within a single large transaction is not reported
 * as stale.
 *
 * @author Chris Cranford
 */
public class VitessOffsetActivityMonitor implements OffsetActivityMonitor<VitessPartition, VitessOffsetContext> {

    private final Duration checkInterval;

    private Vgtid previousRestartVgtid;
    private Vgtid previousCurrentVgtid;

    public VitessOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public StaleOffsetsResult checkForStaleOffsets(VitessPartition partition, VitessOffsetContext offsetContext) {
        final Vgtid restartVgtid = offsetContext.getRestartVgtid();
        final Vgtid currentVgtid = offsetContext.getCurrentVgtid();

        // Check for stale state
        StaleOffsetsResult result = StaleOffsetsResult.fresh();
        if ((restartVgtid != null || currentVgtid != null)
                && Objects.equals(previousRestartVgtid, restartVgtid)
                && Objects.equals(previousCurrentVgtid, currentVgtid)) {
            result = StaleOffsetsResult.stale(
                    ("Offset restart VGTID %s and current VGTID %s have not changed in %d milliseconds. " +
                            "This may indicate the database is idle, there are no changes for the captured tables, " +
                            "or that the connector is no longer receiving change events from the VStream.")
                            .formatted(restartVgtid, currentVgtid, checkInterval.toMillis()));
        }

        // Update tracked stats
        previousRestartVgtid = restartVgtid;
        previousCurrentVgtid = currentVgtid;

        return result;
    }

}