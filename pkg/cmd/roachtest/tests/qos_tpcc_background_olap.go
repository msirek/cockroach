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
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
)

type tpccQosBackgroundOLAPSpec struct {
	Nodes       int
	CPUs        int
	Warehouses  int
	Concurrency int
	Duration    time.Duration
}

func (s tpccQosBackgroundOLAPSpec) run(ctx context.Context, t test.Test, c cluster.Cluster) {

	if c.IsLocal() {
		s.Warehouses = 1
	}
	crdbNodes, workloadNode := setupTPCC(
		ctx, t, c, tpccOptions{
			Warehouses: s.Warehouses, SetupType: usingImport,
		})
	//m := c.NewMonitor(ctx, crdbNodes)
	//m.Go(func(ctx context.Context) error {
	//	t.Status("loading TPCH tables")
	//	cmd := fmt.Sprintf(
	//		"./workload init tpch {pgurl:1-%d} --data-loader=import",
	//		c.Spec().NodeCount-1,
	//	)
	//	c.Run(ctx, workloadNode, cmd)
	//	return nil
	//})
	//m.Wait()
	//m.Go(func(ctx context.Context) error {
	//	t.Status("restoring TPCH dataset for Scale Factor 1")
	//	if err := loadTPCHDataset(ctx, t, c, 1 /* sf */, m, crdbNodes); err != nil {
	//		t.Fatal(err)
	//	}
	//
	//	conn := c.Conn(ctx, t.L(), 1)
	//	defer conn.Close()
	//	if _, err := conn.Exec("USE tpch;"); err != nil {
	//		t.Fatal(err)
	//	}
	//	scatterTables(t, conn, tpchTables)
	//	return nil
	//})
	//m.Wait()

	//m = c.NewMonitor(ctx, crdbNodes)
	m := c.NewMonitor(ctx, crdbNodes)
	//m.Go(func(ctx context.Context) error {
	//	t.Status("running TPCH with concurrency of 4 in the background")
	//	const concurrency = 4
	//	cmd := fmt.Sprintf(
	//		"./workload run tpch {pgurl:1-%d} --display-every=500ms "+
	//			"--concurrency=%d --duration=%s",
	//		c.Spec().NodeCount-1, concurrency, s.Duration,
	//	)
	//	c.Run(ctx, workloadNode, cmd)
	//	return nil
	//})
	// histogramsPath := fmt.Sprintf("%s/stats.json", s.getArtifactsPath())  // msirek-temp
	histogramsPath := t.PerfArtifactsDir() + "/stats.json "
	m.Go(func(ctx context.Context) error {
		t.WorkerStatus("running tpcc with tpch running concurrently")
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
	//snapshots, err := histogram.DecodeSnapshots(histogramsPath)
	//if err != nil {
	//	// If we got this far, and can't decode data, it's not a case of
	//	// overload but something that deserves failing the whole test.
	//	t.Fatal(err)
	//}
	//result := tpcc.NewResultWithSnapshots(s.Warehouses, 0, snapshots)
	//tpmC := result.TpmC()
	//fmt.Println(tpmC)
	verifyNodeLiveness(ctx, c, t, s.Duration)
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
