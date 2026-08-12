/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.vitess;

import java.time.Duration;
import java.util.Objects;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.pipeline.monitor.OffsetActivityMonitor;

/**
 * An {@link OffsetActivityMonitor} that tracks state changes to the connector's offsets.
 * <p>
 * The restart and current VGTIDs are compared against the values captured when the monitor
 * was last consulted, and when neither has moved, a warning is logged. The restart VGTID is
 * the position persisted in the offsets and therefore governs where streaming resumes; it
 * only advances once a transaction has been fully processed, so the current VGTID is
 * additionally compared so that progress within a single large transaction is not reported
 * as stale.
 *
 * @author Chris Cranford
 */
public class VitessOffsetActivityMonitor implements OffsetActivityMonitor<VitessPartition, VitessOffsetContext> {

    private static final Logger LOGGER = LoggerFactory.getLogger(VitessOffsetActivityMonitor.class);

    private final Duration checkInterval;

    private Vgtid previousRestartVgtid;
    private Vgtid previousCurrentVgtid;

    public VitessOffsetActivityMonitor(Duration checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void checkForStaleOffsets(VitessPartition partition, VitessOffsetContext offsetContext) {
        final Vgtid restartVgtid = offsetContext.getRestartVgtid();
        final Vgtid currentVgtid = offsetContext.getCurrentVgtid();

        // Check for stale state
        if ((restartVgtid != null || currentVgtid != null)
                && Objects.equals(previousRestartVgtid, restartVgtid)
                && Objects.equals(previousCurrentVgtid, currentVgtid)) {
            LOGGER.warn("Offset restart VGTID {} and current VGTID {} have not changed in {} milliseconds. " +
                    "This may indicate the database is idle, there are no changes for the captured tables, " +
                    "or that the connector is no longer receiving change events from the VStream.",
                    restartVgtid, currentVgtid, checkInterval.toMillis());
        }

        // Update tracked stats
        previousRestartVgtid = restartVgtid;
        previousCurrentVgtid = currentVgtid;
    }

}