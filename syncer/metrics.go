// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"net/http"
	// For pprof
	_ "net/http/pprof"

	"github.com/ngaut/log"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	binlogEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_events_total",
			Help:      "total number of binlog events",
		}, []string{"type"})

	binlogSkippedEventsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "binlog_skipped_events_total",
			Help:      "total number of skipped binlog events",
		}, []string{"type"})

	sqlJobsTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_jobs_total",
			Help:      "total number of sql jobs",
		}, []string{"type"})

	sqlRetriesTotal = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: "syncer",
			Name:      "sql_retries_total",
			Help:      "total number of sql retryies",
		}, []string{"type"})

	binlogMetaPos = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Namespace: "syncer",
			Name:      "binlog_meta_pos",
			Help:      "current binlog file pos",
		})
)

func initStatusAndMetrics(addr string) {
	prometheus.MustRegister(binlogEventsTotal)
	prometheus.MustRegister(binlogSkippedEventsTotal)
	prometheus.MustRegister(sqlJobsTotal)
	prometheus.MustRegister(sqlRetriesTotal)
	prometheus.MustRegister(binlogMetaPos)

	go func() {
		http.HandleFunc("/status", func(w http.ResponseWriter, req *http.Request) {
			w.Header().Set("Content-Type", "application/text")
			text := GetRawSyncerVersionInfo()
			w.Write([]byte(text))
		})

		// HTTP path for prometheus.
		http.Handle("/metrics", prometheus.Handler())
		log.Infof("listening on %v for status and metrics report.", addr)
		err := http.ListenAndServe(addr, nil)
		if err != nil {
			log.Fatal(err)
		}
	}()
}
