/*
 * PmtMetrics.java
 * Created on Jul 15, 2026, 3:47 PM
 *
 * Copyright 2026 BlueCat Networks (USA) Inc. and its affiliates and licensors. All Rights Reserved.
 *
 * BlueCat Networks and its licensors hereby assert and retain all rights, title, and interest in and to the code,
 * including any and all modifications, enhancements, or derivative works thereof (collectively, the "Code"). This
 * includes, but is not limited to, all intellectual property rights, whether registered or unregistered, associated
 * with the Code. The Code contains trade secrets and proprietary and confidential information of BlueCat Networks
 * and its licensors. It is protected under applicable worldwide copyright and trade secret laws. No rights, title,
 * or interest in the Code are transferred to any third party without the explicit written consent of BlueCat
 * Networks. Any unauthorized use, reproduction, or distribution of the Code is strictly prohibited and may result in
 * legal action.
 */
package com.livingobjects.neo4j.metrics;

import io.prometheus.metrics.exporter.httpserver.HTTPServer;
import io.prometheus.metrics.instrumentation.jvm.JvmMetrics;
import org.neo4j.logging.Log;

import java.io.IOException;

/**
 *
 *
 * @author Baptiste Le Bail
 */
public class PmtMetrics {

    private static final int PORT = 9435;

    private static PmtMetrics INSTANCE;

    public static PmtMetrics get(Log log) {
        if (INSTANCE == null) {
            INSTANCE = new PmtMetrics(log);
        }

        return INSTANCE;
    }

    private PmtMetrics(Log log) {
        try {
            log.info("Will start Prometheus HTTP server on port {}", PORT);
            HTTPServer.builder()
                    .port(PORT)
                    .buildAndStart();
            log.info("Prometheus HTTP server started on port {}", PORT);

            JvmMetrics.builder().register();
        } catch (IOException e) {
            log.error("Could not start Prometheus HTTP server on port {}", PORT, e);
            throw new RuntimeException(e);
        }
    }
}