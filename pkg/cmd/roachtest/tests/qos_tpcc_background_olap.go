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
	gosql "database/sql"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"time"

	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/cluster"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/option"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/registry"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/spec"
	"github.com/cockroachdb/cockroach/pkg/cmd/roachtest/test"
	"github.com/cockroachdb/cockroach/pkg/roachprod/install"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/buildutil"
	"github.com/cockroachdb/cockroach/pkg/util/timeutil"
	"github.com/cockroachdb/cockroach/pkg/workload/histogram"
	"github.com/cockroachdb/cockroach/pkg/workload/tpcc"
	"github.com/cockroachdb/ttycolor"
	"github.com/stretchr/testify/require"
)

type tpccQoSBackgroundOLAPSpec struct {
	name            string
	Nodes           int
	CPUs            int
	Warehouses      int
	Concurrency     int
	OLAPConcurrency int
	Duration        time.Duration
	Ramp            time.Duration
	numRuns         int

	acEnabled   bool
	readPercent int
	blockSize   int
	duration    time.Duration
	//concurrency func(int) int
	batchSize  int
	maxLoadOps int
}

type mtFairnessSpec struct {
	name        string
	acEnabled   bool
	readPercent int
	blockSize   int
	duration    time.Duration
	concurrency func(int) int
	batchSize   int
	maxLoadOps  int
}

func registerMultiTenantFairness(r registry.Registry) {
	acStr := map[bool]string{
		true:  "admission",
		false: "no-admission",
	}
	for _, acEnabled := range []bool{true, false} {
		kvSpecs := []tpccQoSBackgroundOLAPSpec{
			{
				name:        "same",
				concurrency: func(int) int { return 250 },
			},
			{
				name:        "concurrency-skew",
				concurrency: func(i int) int { return i * 250 },
			},
		}
		for i := range kvSpecs {
			s := kvSpecs[i]
			s.blockSize = 5
			s.readPercent = 95
			s.acEnabled = acEnabled
			s.duration = 5 * time.Minute
			s.batchSize = 100
			s.maxLoadOps = 100_000

			r.Add(registry.TestSpec{
				Name:              fmt.Sprintf("multitenant/fairness/kv/%s/%s", s.name, acStr[s.acEnabled]),
				Cluster:           r.MakeClusterSpec(5),
				Owner:             registry.OwnerSQLQueries,
				NonReleaseBlocker: false,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantFairness(ctx, t, c, s, "SELECT k, v FROM kv")
				},
			})
		}
		storeSpecs := []mtFairnessSpec{
			{
				name:        "same",
				concurrency: func(i int) int { return 50 },
			},
			{
				name:        "concurrency-skew",
				concurrency: func(i int) int { return i * 50 },
			},
		}
		for i := range storeSpecs {
			s := storeSpecs[i]
			s.blockSize = 50_000
			s.readPercent = 5
			s.acEnabled = acEnabled
			s.duration = 10 * time.Minute
			s.batchSize = 1
			s.maxLoadOps = 1000

			r.Add(registry.TestSpec{
				Name:              fmt.Sprintf("multitenant/fairness/store/%s/%s", s.name, acStr[s.acEnabled]),
				Cluster:           r.MakeClusterSpec(5),
				Owner:             registry.OwnerSQLQueries,
				NonReleaseBlocker: false,
				Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
					runMultiTenantFairness(ctx, t, c, s, "UPSERT INTO kv(k, v)")
				},
			})
		}
	}
	if buildutil.CrdbTestBuild {
		quick := mtFairnessSpec{
			duration:    1,
			acEnabled:   false,
			readPercent: 95,
			name:        "quick",
			concurrency: func(i int) int { return 1 },
			blockSize:   2,
			batchSize:   10,
			maxLoadOps:  1000,
		}
		r.Add(registry.TestSpec{
			Name:              "multitenant/fairness/quick",
			Cluster:           r.MakeClusterSpec(2),
			Owner:             registry.OwnerSQLQueries,
			NonReleaseBlocker: false,
			Run: func(ctx context.Context, t test.Test, c cluster.Cluster) {
				runMultiTenantFairness(ctx, t, c, quick, "SELECT k, v FROM kv")
			},
		})
	}
}

func runMultiTenantFairness(
	ctx context.Context, t test.Test, c cluster.Cluster, s tpccQoSBackgroundOLAPSpec, query string,
) {
	numNodes := c.Spec().NodeCount - 1

	duration := s.duration

	// For quick local testing.
	quick := c.IsLocal() || s.name == "quick"
	if quick {
		duration = 30 * time.Second
		s.Concurrency = 4
	}

	c.Put(ctx, t.Cockroach(), "./cockroach")
	c.Start(ctx, t.L(), option.DefaultStartOpts(), install.MakeClusterSettings(install.SecureOption(true)), c.Node(c.Spec().NodeCount-1))
	SetAdmissionControl(ctx, t, c, true)

	setRateLimit := func(ctx context.Context, val int, node int) {
		db := c.Conn(ctx, t.L(), node)
		defer db.Close()
		if _, err := db.ExecContext(
			ctx, fmt.Sprintf("SET CLUSTER SETTING kv.tenant_rate_limiter.rate_limit = '%d'", val)); err != nil {
			t.Fatalf("failed to set kv.tenant_rate_limiter.rate_limit: %v", err)
		}
	}
	for i := 1; i <= c.Spec().NodeCount-1; i++ {
		setRateLimit(ctx, 1000000, i)
	}

	const (
		nodeBaseHTTPPort = 8081
		nodeBaseSQLPort  = 26257
	)

	nodeHTTPPort := func(offset int) int {
		if c.IsLocal() {
			return nodeBaseHTTPPort + offset
		}
		return nodeBaseHTTPPort
	}
	nodeSQLPort := func(offset int) int {
		if c.IsLocal() {
			return nodeBaseSQLPort + offset
		}
		return nodeBaseSQLPort
	}

	// Init kv from the driver node.
	cmd := fmt.Sprintf("./cockroach workload init kv {pgurl:1-%d} --secure", c.Spec().NodeCount-1)
	err := c.RunE(ctx, c.Node(c.Spec().NodeCount), cmd)
	require.NoError(t, err)

	// This doesn't work on tenant, have to do it on kvserver
	//setRateLimit(ctx, 1000000, node)
	//  failed to set range_max_bytes: pq: unimplemented: operation is unsupported in multi-tenancy mode
	//setMaxRangeBytes(ctx, 1<<18, node)

	m := c.NewMonitor(ctx, c.Node(c.Spec().NodeCount))

	//t.L().Printf("running dataload 	")

	m.Go(func(ctx context.Context) error {
		cmd := fmt.Sprintf(
			"./cockroach workload run kv {pgurl:1-%d} --secure --min-block-bytes %d --max-block-bytes %d "+
				"--batch %d --max-ops %d --concurrency=100",
			c.Spec().NodeCount-1, s.blockSize, s.blockSize, s.batchSize, s.maxLoadOps)
		err := c.RunE(ctx, c.Node(c.Spec().NodeCount), cmd)
		t.L().Printf("dataload done\n")
		return err
	})

	m.Wait()
	t.L().Printf("running main workloads")
	m = c.NewMonitor(ctx, c.Node(c.Spec().NodeCount))

	m.Go(func(ctx context.Context) error {
		cmd := fmt.Sprintf(
			"./cockroach workload run kv {pgurl:1-%d} --write-seq=%s --secure --min-block-bytes %d "+
				"--max-block-bytes %d --batch %d --duration=%s --read-percent=%d --concurrency=%d",
			c.Spec().NodeCount-1, fmt.Sprintf("R%d", s.maxLoadOps*s.batchSize), s.blockSize, s.blockSize, s.batchSize,
			duration, s.readPercent, s.Concurrency)
		err := c.RunE(ctx, c.Node(c.Spec().NodeCount), cmd)
		return err
	})

	m.Wait()
	t.L().Printf("workloads done")

	// Pull workload performance from crdb_internal.statement_statistics. Alternatively we could pull these from
	// workload but this seemed most straightforward.
	var counts float64
	var meanLatency float64
	counts := make([]float64, numNodes)
	meanLatencies := make([]float64, numNodes)

	db, err := gosql.Open("postgres", tenants[i].pgURL)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()
	tdb := sqlutils.MakeSQLRunner(db)
	tdb.Exec(t, "USE kv")
	querySelector := fmt.Sprintf(`{"querySummary": "%s"}`, query)
	// TODO: should we check that count of failed queries is smallish?
	rows := tdb.Query(t, `
			SELECT
				sum((statistics -> 'statistics' -> 'cnt')::INT),
				avg((statistics -> 'statistics' -> 'runLat' -> 'mean')::FLOAT)
			FROM crdb_internal.statement_statistics
			WHERE metadata @> '{"db":"kv","failed":false}' AND metadata @> $1`, querySelector)

	if rows.Next() {
		var cnt, lat float64
		err := rows.Scan(&cnt, &lat)
		require.NoError(t, err)
		counts[i] = cnt
		meanLatencies[i] = lat
	} else {
		t.Fatal("no query results")
	}

	failThreshold := .3

	throughput := make([]float64, numTenants)
	ok, maxThroughputDelta := floatsWithinPercentage(counts, failThreshold)
	for i, count := range counts {
		throughput[i] = count / duration.Seconds()
	}

	if s.acEnabled && !ok {
		t.L().Printf("Throughput not within expectations: %f > %f %v", maxThroughputDelta, failThreshold, throughput)
	}

	t.L().Printf("Max throughput delta: %d%% %d %v\n", int(maxThroughputDelta*100), average(throughput), counts)

	ok, maxLatencyDelta := floatsWithinPercentage(meanLatencies, failThreshold)
	t.L().Printf("Max latency delta: %d%% %v\n", int(maxLatencyDelta*100), meanLatencies)

	if s.acEnabled && !ok {
		t.L().Printf("Latency not within expectations: %f > %f %v", maxLatencyDelta, failThreshold, meanLatencies)
	}

	c.Run(ctx, c.Node(1), "mkdir", "-p", t.PerfArtifactsDir())
	results := fmt.Sprintf(`{ "max_tput_delta": %f, "max_tput": %f, "min_tput": %f, "max_latency": %f, "min_latency": %f}`,
		maxThroughputDelta, maxFloat(throughput), minFloat(throughput), maxFloat(meanLatencies), minFloat(meanLatencies))
	t.L().Printf("reporting perf results: %s", results)
	cmd := fmt.Sprintf(`echo '%s' > %s/stats.json`, results, t.PerfArtifactsDir())
	c.Run(ctx, c.Node(1), cmd)

	// get cluster timeseries data into artifacts
	err := c.FetchTimeseriesData(ctx, t)
	require.NoError(t, err)
}

func average(values []float64) int {
	average := 0
	for _, v := range values {
		average += int(v)
	}
	average /= len(values)
	return average
}

func minFloat(values []float64) float64 {
	min := values[0]
	for _, v := range values {
		if v < min {
			min = v
		}
	}
	return min
}

func maxFloat(values []float64) float64 {
	max := values[0]
	for _, v := range values {
		if v > max {
			max = v
		}
	}
	return max
}

func floatsWithinPercentage(values []float64, percent float64) (bool, float64) {
	average := 0.0
	for _, v := range values {
		average += v
	}
	average = average / float64(len(values))
	limit := average * percent
	maxDelta := 0.0
	for _, v := range values {
		delta := math.Abs(average - v)
		if delta > limit {
			return false, 1.0 - (average-delta)/average
		}
		if delta > maxDelta {
			maxDelta = delta
		}
	}
	// make a percentage
	maxDelta = 1.0 - (average-maxDelta)/average
	return true, maxDelta
}

// run sets up TPCC then runs both TPCC plus TPCH OLAP queries concurrently,
// first with the OLAP queries using `background` transaction quality of
// service, then with OLAP queries using `regular` transaction quality of
// service.  The results are logged and compared. The test fails if using
// `background` QoS doesn't result in a tpmC score from TPCC which is at least a
// certain percentage higher than that when OLAP queries are run with `regular`
// QoS.
func (s tpccQoSBackgroundOLAPSpec) run(ctx context.Context, t test.Test, c cluster.Cluster) {

	histogramsPath := t.PerfArtifactsDir() + "/stats.json"
	histogramsPathQoS := t.PerfArtifactsDir() + "/stats_qos.json"

	var throttledOLAPTpmC, throttledOLAPTPCCEfficiency []float64
	var noThrottleTpmC, noThrottleTPCCEfficiency []float64
	throttledOLAPTpmC = make([]float64, 0, s.numRuns)
	throttledOLAPTPCCEfficiency = make([]float64, 0, s.numRuns)
	noThrottleTpmC = make([]float64, 0, s.numRuns)
	noThrottleTPCCEfficiency = make([]float64, 0, s.numRuns)
	var tpmCQoS, tpmCNoQoS float64
	var efficiencyCQoS, efficiencyNoQoS float64
	var baselineTpmC, baselineEfficiency float64

	for i := 0; i < s.numRuns; i++ {
		crdbNodes, workloadNode := s.setupDatabases(ctx, t, c)

		s.runTPCC(ctx, t, c, crdbNodes, workloadNode, histogramsPath, false /* useBackgroundQoS */)
		baselineTpmC, baselineEfficiency, _ =
			s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPathQoS, true /* withQoS */)

		startQoS := timeutil.Now()
		s.runTPCCAndOLAPQueries(ctx, t, c, crdbNodes, workloadNode, histogramsPathQoS, true /* useBackgroundQoS */)
		endQoS := timeutil.Now()
		// Get the TPCC perf and efficiency when other OLAP queries are run with
		// background QoS.
		tpmC, efficiency, throttledResult :=
			s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPathQoS, true /* withQoS */)
		tpmCQoS += tpmC
		efficiencyCQoS += efficiency
		throttledOLAPTpmC = append(throttledOLAPTpmC, tpmC)
		throttledOLAPTPCCEfficiency = append(throttledOLAPTPCCEfficiency, efficiency)

		startNoQoS := timeutil.Now()
		s.runTPCCAndOLAPQueries(ctx, t, c, crdbNodes, workloadNode, histogramsPath, false /* useBackgroundQoS */)
		endNoQoS := timeutil.Now()

		// Get the TPCC perf and efficiency when other OLAP queries are run with
		// regular QoS.
		tpmC, efficiency, noThrottleResult :=
			s.getTpmcAndEfficiency(ctx, t, c, workloadNode, histogramsPath, false /* withQoS */)
		tpmCNoQoS += tpmC
		efficiencyNoQoS += efficiency
		noThrottleTpmC = append(noThrottleTpmC, tpmC)
		noThrottleTPCCEfficiency = append(noThrottleTPCCEfficiency, efficiency)

		printResults(throttledResult, noThrottleResult, t)

		t.L().Printf("\n")
		t.L().Printf("Admission control metrics without QoS\n")
		t.L().Printf("-------------------------------------\n")
		printAdmissionMetrics(ctx, c, t, c.Node(1), startNoQoS, endNoQoS)
		t.L().Printf("\n")
		t.L().Printf("Admission control metrics with QoS\n")
		t.L().Printf("----------------------------------\n")
		printAdmissionMetrics(ctx, c, t, c.Node(1), startQoS, endQoS)
		t.L().Printf("\n")
	}
	efficiencyNoQoS /= float64(s.numRuns)
	efficiencyCQoS /= float64(s.numRuns)
	tpmCNoQoS /= float64(s.numRuns)
	tpmCQoS /= float64(s.numRuns)

	// Test results vary. Allow at most a 5% regression or random variance in the
	// run using background QoS.
	const maxAllowedRegression = -5.0
	percentImprovement := 100.0 * (tpmCQoS - tpmCNoQoS) / tpmCNoQoS
	t.L().Printf("tpmC_No_Tpch:         %.2f   Efficiency: %.4v\n",
		baselineTpmC, baselineEfficiency)
	t.L().Printf("tpmC_No_QoS:         %.2f   Efficiency: %.4v\n",
		tpmCNoQoS, efficiencyNoQoS)
	t.L().Printf("tpmC_Background_QoS: %.2f   Efficiency: %.4v\n",
		tpmCQoS, efficiencyCQoS)
	var scoreDelta string
	if percentImprovement < 0.0 {
		scoreDelta = fmt.Sprintf("%.1f%% lower", -percentImprovement)
	} else {
		scoreDelta = fmt.Sprintf("%.1f%% higher", percentImprovement)
	}
	message := fmt.Sprintf(
		`TPCC run in parallel with OLAP queries using background QoS
                                                   had a tpmC score %s than with regular QoS.`,
		scoreDelta)
	if percentImprovement < maxAllowedRegression {
		ttycolor.Stdout(ttycolor.Red)
		failMessage := fmt.Sprintf("FAIL: %s\n", message)
		t.L().Printf(failMessage)
		ttycolor.Stdout(ttycolor.Reset)
		t.Fatalf(failMessage)
	} else {
		ttycolor.Stdout(ttycolor.Green)
		t.L().Printf("SUCCESS: %s\n", message)
	}
	ttycolor.Stdout(ttycolor.Reset)
}

func printAdmissionMetrics(
	ctx context.Context,
	c cluster.Cluster,
	t test.Test,
	adminNode option.NodeListOption,
	start, end time.Time,
) {
	// Query needed information over the timespan of the query.
	adminUIAddrs, err := c.ExternalAdminUIAddr(ctx, t.L(), adminNode)
	if err != nil {
		t.Fatal(err)
	}
	var admissionMetrics = []tsQuery{
		// Work Queue Admission Counter
		{name: "admission.requested.kv", queryType: total},
		{name: "admission.admitted.kv", queryType: total},
		{name: "admission.errored", queryType: total},
		{name: "admission.requested.kv-stores", queryType: total},
		{name: "admission.admitted.kv-stores", queryType: total},
		{name: "admission.errored.kv-stores", queryType: total},
		{name: "admission.requested.sql-kv-response", queryType: total},
		{name: "admission.admitted.sql-kv-response", queryType: total},
		{name: "admission.errored.sql-kv-response", queryType: total},
		{name: "admission.requested.sql-sql-response", queryType: total},
		{name: "admission.admitted.sql-sql-response", queryType: total},
		{name: "admission.errored.sql-sql-response", queryType: total},
		{name: "admission.requested.sql-leaf-start", queryType: total},
		{name: "admission.admitted.sql-leaf-start", queryType: total},
		{name: "admission.errored.sql-leaf-start", queryType: total},
		{name: "admission.requested.sql-root-start", queryType: total},
		{name: "admission.admitted.sql-root-start", queryType: total},
		{name: "admission.errored.sql-root-start", queryType: total},
		// Work Queue Length
		{name: "admission.wait_queue_length.kv", queryType: total},
		{name: "admission.wait_queue_length.kv-stores", queryType: total},
		{name: "admission.wait_queue_length.sql-kv-response", queryType: total},
		{name: "admission.wait_queue_length.sql-sql-response", queryType: total},
		{name: "admission.wait_queue_length.sql-leaf-start", queryType: total},
		{name: "admission.wait_queue_length.sql-root-start", queryType: total},

		// Work Queue Admission Latency Sum
		{name: "admission.wait_sum.kv", queryType: total},
		{name: "admission.wait_sum.kv-stores", queryType: total},
		{name: "admission.wait_sum.sql-kv-response", queryType: total},
		{name: "admission.wait_sum.sql-sql-response", queryType: total},
		{name: "admission.wait_sum.sql-leaf-start", queryType: total},
		{name: "admission.wait_sum.sql-root-start", queryType: total},
		// Granter
		{name: "admission.granter.total_slots.kv", queryType: total},
		{name: "admission.granter.used_slots.kv", queryType: total},
		{name: "admission.granter.used_slots.sql-leaf-start", queryType: total},
		{name: "admission.granter.used_slots.sql-root-start", queryType: total},

		// IO Tokens Exhausted Duration Sum
		{name: "admission.granter.io_tokens_exhausted_duration.kv", queryType: total},
	}

	adminURL := adminUIAddrs[0]
	response := mustGetMetrics(t, adminURL, start, end, admissionMetrics)

	// Drop the first two minutes of datapoints as a "ramp-up" period.
	//perMinute := response.Results[0].Datapoints[2:]
	//cumulative := response.Results[1].Datapoints[2:]  // msirek-temp

	t.L().Printf("\n")
	for i, metric := range admissionMetrics {
		lastIdx := len(response.Results[i].Datapoints) - 1
		if lastIdx < 1 {
			continue
		}
		dataPoints := response.Results[i].Datapoints
		totalCount := dataPoints[lastIdx].Value - dataPoints[0].Value
		t.L().Printf("s: %f\n", metric.name, totalCount)
	}
	t.L().Printf("\n")
}

func (s tpccQoSBackgroundOLAPSpec) setupDatabases(
	ctx context.Context, t test.Test, c cluster.Cluster,
) (crdbNodes, workloadNode option.NodeListOption) {
	// Set up TPCC tables.
	crdbNodes, workloadNode = setupTPCC(
		ctx, t, c, tpccOptions{
			Warehouses: s.Warehouses, SetupType: usingImport, DontOverrideWarehouses: true,
		})
	/*
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
		// msirek-temp
	*/
	return crdbNodes, workloadNode
}

func (s tpccQoSBackgroundOLAPSpec) runTPCCAndOLAPQueries(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	crdbNodes, workloadNode option.NodeListOption,
	histogramsPath string,
	useBackgroundQoS bool,
) {
	m := c.NewMonitor(ctx, crdbNodes)
	// Kick off TPC-H with concurrency.
	/*
		m.Go(func(ctx context.Context) error {
			var backgroundQoSOpt string
			message := fmt.Sprintf("running TPCH with concurrency of %d", s.OLAPConcurrency)
			if useBackgroundQoS {
				message += " with background quality of service"
				backgroundQoSOpt = "--background-qos"
			}
			t.Status(message)
			cmd := fmt.Sprintf(
				"./cockroach workload run tpch {pgurl:1-%d} --tolerate-errors "+
					"--concurrency=%d --duration=%s %s",
				c.Spec().NodeCount-1, s.OLAPConcurrency, s.Duration+s.Ramp, backgroundQoSOpt,
			)
			c.Run(ctx, workloadNode, cmd)
			return nil
		})

	*/
	s.runTPCC(ctx, t, c, crdbNodes, workloadNode, histogramsPath, useBackgroundQoS)
	m.Wait()
}

func (s tpccQoSBackgroundOLAPSpec) runTPCC(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	crdbNodes, workloadNode option.NodeListOption,
	histogramsPath string,
	useBackgroundQoS bool,
) {
	// Kick off TPC-C
	m := c.NewMonitor(ctx, crdbNodes)
	m.Go(func(ctx context.Context) error {
		message := "running tpcc"
		t.WorkerStatus(message)
		cmd := fmt.Sprintf(
			"./workload run tpcc"+
				" --tolerate-errors"+
				" --warehouses=%d"+
				" --concurrency=%d"+
				" --histograms=%s "+
				" --ramp=%s "+
				" --duration=%s {pgurl:1-%d}",
			s.Warehouses, s.Concurrency, histogramsPath, s.Ramp, s.Duration, c.Spec().NodeCount-1)
		c.Run(ctx, workloadNode, cmd)
		return nil
	})
	m.Wait()
}

func (s tpccQoSBackgroundOLAPSpec) getTpmcAndEfficiency(
	ctx context.Context,
	t test.Test,
	c cluster.Cluster,
	workloadNode option.NodeListOption,
	histogramsPath string,
	withQoS bool,
) (tpmC float64, efficiency float64, result *tpcc.Result) {
	var fileName string
	if withQoS {
		fileName = "stats_qos.json"
	} else {
		fileName = "stats.json"
	}
	localHistPath := filepath.Join(t.ArtifactsDir(), fileName)
	// Copy the performance results from the workloadNode to the local system
	// where roachtest is being run.
	if err := c.Get(ctx, t.L(), histogramsPath, localHistPath, workloadNode); err != nil {
		t.Fatal(err)
	}

	snapshots, err := histogram.DecodeSnapshots(localHistPath)
	if err != nil {
		t.Fatal(err)
	}
	result = tpcc.NewResultWithSnapshots(s.Warehouses, 0, snapshots)
	tpmC = result.TpmC()
	efficiency = result.Efficiency()
	return tpmC, efficiency, result
}

func printResults(throttledResult *tpcc.Result, noThrottleResult *tpcc.Result, t test.Test) {
	t.L().Printf("\n")
	t.L().Printf("TPCC results with OLAP queries running simultaneously\n")
	t.L().Printf("-----------------------------------------------------\n")
	printOneResult(noThrottleResult, t)
	t.L().Printf("\n\n")
	t.L().Printf("TPCC results with OLAP queries running simultaneously with background QoS\n")
	t.L().Printf("-------------------------------------------------------------------------\n")
	printOneResult(throttledResult, t)
	t.L().Printf("\n\n")
}

func printOneResult(res *tpcc.Result, t test.Test) {
	t.L().Printf("Duration: %.5v, Warehouses: %v, Efficiency: %.4v, tpmC: %.2f\n",
		res.Elapsed, res.ActiveWarehouses, res.Efficiency(), res.TpmC())
	t.L().Printf("_elapsed___ops/sec(cum)__p50(ms)__p90(ms)__p95(ms)__p99(ms)_pMax(ms)\n")

	var queries []string
	for query := range res.Cumulative {
		queries = append(queries, query)
	}
	sort.Strings(queries)
	for _, query := range queries {
		hist := res.Cumulative[query]
		t.L().Printf("%7.1fs %14.1f %8.1f %8.1f %8.1f %8.1f %8.1f %s\n",
			res.Elapsed.Seconds(),
			float64(hist.TotalCount())/res.Elapsed.Seconds(),
			time.Duration(hist.ValueAtQuantile(50)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(90)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(95)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(99)).Seconds()*1000,
			time.Duration(hist.ValueAtQuantile(100)).Seconds()*1000,
			query,
		)
	}
}

func (s tpccQoSBackgroundOLAPSpec) getArtifactsPath() string {
	return fmt.Sprintf("qos/tpcc_background_olap/nodes=%d/cpu=%d/w=%d/c=%d",
		s.Nodes, s.CPUs, s.Warehouses, s.Concurrency)
}

func registerTPCCQoSBackgroundOLAPSpec(r registry.Registry, s tpccQoSBackgroundOLAPSpec) {
	name := s.getArtifactsPath()
	r.Add(registry.TestSpec{
		Name:    name,
		Owner:   registry.OwnerSQLQueries,
		Cluster: r.MakeClusterSpec(s.Nodes+1, spec.CPU(s.CPUs)),
		Run:     s.run,
		Timeout: 1 * time.Hour,
	})
}

func registerTPCCQoSBackgroundOLAP(r registry.Registry) {
	specs := []tpccQoSBackgroundOLAPSpec{
		{
			name:            "baseline",
			CPUs:            32,
			Concurrency:     500,
			OLAPConcurrency: 64,
			Nodes:           3,
			Warehouses:      1,
			Duration:        10 * time.Minute,
			Ramp:            1 * time.Minute,
			numRuns:         1,
		},
	}
	for _, s := range specs {
		registerTPCCQoSBackgroundOLAPSpec(r, s)
	}
}
