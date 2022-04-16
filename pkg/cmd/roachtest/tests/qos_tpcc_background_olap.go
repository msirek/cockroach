// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package tests

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
)

type tpccQosBackgroundOLAPSpec struct {
	Nodes       int
	CPUs        int
	Warehouses  int
	Concurrency int
	Duration    time.Duration
}

func (s tpccQosBackgroundOLAPSpec) runTpccAndTpch(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	crdbNodes, workloadNode option.NodeListOption,
	histogramsPath string,
	useBackgroundQoS bool,
) {
	m := c.NewMonitor(ctx, crdbNodes)

	// Kick off TPC-H with concurrency
	m.Go(func(ctx context.Context) error {
		var backgroundQoSOpt string
		const concurrency = 4
		message := fmt.Sprintf("running TPCH with concurrency of %d", concurrency)
		if useBackgroundQoS {
			message += " with background quality of service"
			backgroundQoSOpt = "--background-qos"
		}
		t.Status(message)
		cmd := fmt.Sprintf(
			"./workload run tpch {pgurl:1-%d} --display-every=500ms "+
				"--concurrency=%d --duration=%s %s",
			c.Spec().NodeCount-1, concurrency, s.Duration, backgroundQoSOpt,
		)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})

	// Kick off TPC-C with no concurrency
	m.Go(func(ctx context.Context) error {
		message := "running tpcc with tpch running concurrently"
		t.WorkerStatus(message)
		cmd := fmt.Sprintf(
			"./workload run tpcc"+
				" --warehouses=%d"+
				" --concurrency=%d"+
				" --histograms=%s "+
				" --duration=%s {pgurl:1-%d}",
			s.Warehouses, s.Concurrency, histogramsPath, s.Duration, c.Spec().NodeCount-1)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	m.Wait()
}

func (s tpccQosBackgroundOLAPSpec) getTpmcAndEfficiency(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	workloadNode option.NodeListOption,
	histogramsPath string,
) (tpmC float64, efficiency float64) {
	localHistPath := filepath.Join(t.ArtifactsDir(), "/stats.json")
	// Copy the performance results from the workloadNode to the local system
	// where roachtest is being run.
	if err := c.Get(ctx, t.L(), histogramsPath, localHistPath, workloadNode); err != nil {
		t.Fatal(err)
	}

	snapshots, err := histogram.DecodeSnapshots(localHistPath)
	if err != nil {
		t.Fatal(err)
	}
	result := tpcc.NewResultWithSnapshots(s.Warehouses, 0, snapshots)
	tpmC = result.TpmC()
	efficiency = result.Efficiency()
	return tpmC, efficiency
}

func (s tpccQosBackgroundOLAPSpec) run(ctx context.Context, t test.Test, c cluster.Cluster) {
	if c.IsLocal() {
		s.Warehouses = 1
	}

	// Set up TPCC tables.
	crdbNodes, workloadNode := setupTPCC(
		ctx, t, c, tpccOptions{
			Warehouses: s.Warehouses, SetupType: usingImport,
		})
	m := c.NewMonitor(ctx, crdbNodes)
	// Set up TPCH tables.
	m.Go(func(ctx context.Context) error {
		t.Status("loading TPCH tables")
		cmd := fmt.Sprintf(
			"./workload init tpch {pgurl:1-%d} --data-loader=import",
			c.Spec().NodeCount-1,
		)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	m.Wait()

	histogramsPath := t.PerfArtifactsDir() + "/stats.json"
	s.runTpccAndTpch(ctx, t, c, crdbNodes, workloadNode, histogramsPath, false)
	// Get the TPCC perf and efficiency when TPCH is run at normal QoS.
	noThrottleTpmC, noThrottleTpccEfficiency :=
		s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPath)

	s.runTpccAndTpch(ctx, t, c, crdbNodes, workloadNode, histogramsPath, true)
	// Get the TPCC perf and efficiency when TPCH is run at background QoS.
	throttledOlapTpmC, throttledOlapTpccEfficiency :=
		s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPath)

	fmt.Println(noThrottleTpmC, noThrottleTpccEfficiency)
	fmt.Println(throttledOlapTpmC, throttledOlapTpccEfficiency)
}

func (s tpccQosBackgroundOLAPSpec) getArtifactsPath() string {
	return fmt.Sprintf("qos/tpcc_background_olap/nodes=%d/cpu=%d/w=%d/c=%d",
		s.Nodes, s.CPUs, s.Warehouses, s.Concurrency)
}

func registerTPCCQoSBackgroundOLAPSpec(r registry.Registry, s tpccQosBackgroundOLAPSpec) {
	name := s.getArtifactsPath()
	r.Add(registry.TestSpec{
		Name:    name,
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(s.Nodes+1, spec.CPU(s.CPUs)),
		Run:     s.run,
		Timeout: 20 * time.Minute,
	})
}

func registerTPCCQoSBackgroundOLAP(r registry.Registry) {
	specs := []tpccQosBackgroundOLAPSpec{
		{
			CPUs:        8,
			Concurrency: 1,
			Nodes:       3,
			Warehouses:  30,
			Duration:    3 * time.Minute, // msirek-temp
			//Duration: 1 * time.Minute,
		},
	}
	for _, s := range specs {
		registerTPCCQoSBackgroundOLAPSpec(r, s)
	}
}
