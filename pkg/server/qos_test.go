// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package server

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

// This benchmark tests the effectiveness of the session setting
// `SET default_transaction_quality_of_service=background|regular|critical`
// at prioritizing work done on behalf of SQL statements.
//
// The following three constants can be modified for comparing one
// benchmark run with another. The first two adjust the QoS level. The last
// one can be adjusted if there is too much variance seen in test runs.

// The Quality of Service level to use for background SQLs competing for
// resources with the other SQLs we are benchmarking. CHANGE THIS and compare
// runtimes. Valid values: Background, Regular, Critical
const BackgroundSqlQoSLevel = Regular

// The Quality of Service level to use for the SQLs we are benchmarking.
// CHANGE THIS along with backgroundSqlQoSLevel for comparative benchmarking.
// Valid values: Background, Regular, Critical
const BenchmarkSqlQoSLevel = Regular

// Adjusts the CPU contention by specifying the number of simultaneous background
// queries to run.  CHANGE THIS and compare runtimes.
const BackgroundSqlNumQueries = 4

// Define which tables to use for the background queries and benchmark queries.
const backgroundTableName = `t.a1`
const benchTableName = `t.a3`

var tableDefMap = map[string]string{
	"t.a1": `CREATE TABLE t.a1 (k INT PRIMARY KEY USING HASH WITH
                                              BUCKET_COUNT = 256, v CHAR(3));`,
	"t.a2": `CREATE TABLE t.a2 (k INT PRIMARY KEY, v CHAR(3));`,
	"t.a3": `CREATE TABLE t.a3 (k INT PRIMARY KEY, v CHAR(3));`,
	"t.a4": `CREATE TABLE t.a4 (k INT, v CHAR(3));`,
}

type QoSUserLevel sessiondatapb.QoSLevel

const (
	Background = QoSUserLevel(sessiondatapb.UserLow)
	Regular    = QoSUserLevel(sessiondatapb.Normal)
	Critical   = QoSUserLevel(sessiondatapb.UserHigh)
)

var qosSetStmtDict = map[QoSUserLevel]string{
	Background: `SET default_transaction_quality_of_service=background; `,
	Regular:    `SET default_transaction_quality_of_service=regular; `,
	Critical:   `SET default_transaction_quality_of_service=critical; `,
}

type qosBenchmarkParams struct {
	stopper                 *stop.Stopper
	gatewayServer           *TestServer
	ctx                     context.Context
	sqlRun                  *sqlutils.SQLRunner
	backgroundSqlQoSLevel   QoSUserLevel
	backgroundSqlStmt       string
	backgroundSqlNumQueries int
}

func printRunInfo(
	backgroundTableName string,
	benchTableName string,
	loadBGTableStmt string,
	loadBenchTableStmt string,
	oltpBenchQuery string,
	olapBenchQuery string,
	dmlBenchStmt string,
	olapBackgroundQueryStmt string,
) {
	var backgroundSqlQoSLevelString string
	switch BackgroundSqlQoSLevel {
	case Background:
		backgroundSqlQoSLevelString = "background"
	case Regular:
		backgroundSqlQoSLevelString = "regular"
	case Critical:
		backgroundSqlQoSLevelString = "critical"
	}
	var benchmarkSqlQoSLevelString string
	switch BenchmarkSqlQoSLevel {
	case Background:
		benchmarkSqlQoSLevelString = "background"
	case Regular:
		benchmarkSqlQoSLevelString = "regular"
	case Critical:
		benchmarkSqlQoSLevelString = "critical"
	}
	fmt.Println("———————————————————————————————————————————————————————————————————————————————")
	fmt.Printf("BackgroundSqlQoSLevel:   %s     ", backgroundSqlQoSLevelString)
	fmt.Printf("BenchmarkSqlQoSLevel: %s     \n", benchmarkSqlQoSLevelString)
	fmt.Printf("backgroundTableDef:      %s\n", tableDefMap[backgroundTableName])
	fmt.Printf("benchTableDef:           %s\n", tableDefMap[benchTableName])
	fmt.Println()
	fmt.Println(loadBGTableStmt)
	fmt.Println(loadBenchTableStmt)
	fmt.Println()
	fmt.Printf("Background SQL: %s\n", olapBackgroundQueryStmt)
	fmt.Printf("Number instances of Background SQL running in parallel: %d\n",
		BackgroundSqlNumQueries)
	fmt.Println()
	fmt.Printf("OLTP Query: %s\n", oltpBenchQuery)
	fmt.Printf("OLAP Query: %s\n", olapBenchQuery)
	fmt.Printf("OLTP DML  : %s\n", dmlBenchStmt)
	fmt.Println("———————————————————————————————————————————————————————————————————————————————")
}

func startBackgroundSQL(b *testing.B, params qosBenchmarkParams) {
	for j := 0; j < params.backgroundSqlNumQueries; j++ {
		_ = params.stopper.RunAsyncTask(params.ctx, "Background Query", func(ctx context.Context) {
			sqlDB := serverutils.OpenDBConn(
				b, params.gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
				params.stopper)
			defer sqlDB.Close()
			sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
			backgroundSqlSetQoSStmt, _ := qosSetStmtDict[params.backgroundSqlQoSLevel]
			sqlRunner.Exec(b, backgroundSqlSetQoSStmt)
			for {
				select {
				case <-params.stopper.ShouldQuiesce():
					return
				case <-params.stopper.IsStopped():
					return
				default:
				}
				rows := sqlRunner.Query(b, params.backgroundSqlStmt)
				rows.Close()
			}
		},
		)
	}
}

func benchQueryWithQoS(params qosBenchmarkParams, numOps int, queryStmt string) func(b *testing.B) {
	return func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				rows := params.sqlRun.Query(b, queryStmt)
				rows.Close()
			}
		}
		b.StopTimer()
	}
}

func benchDMLWithQoS(params qosBenchmarkParams, numOps int, dmlStmt string) func(b *testing.B) {
	return func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				params.sqlRun.Exec(b, dmlStmt)
			}
		}
		b.StopTimer()
	}
}

func BenchmarkQoS(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	// TODO(msirek): Reduce these budget sizes when memory accounting is
	params.SQLMemoryPoolSize = 8 << 30
	params.TempStorageConfig =
		base.DefaultTestTempStorageConfigWithSize(cluster.MakeTestingClusterSettings(), 1<<30)
	tc := serverutils.StartNewTestCluster(b, 1, /* numNodes */
		base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	gatewayServer := tc.Server(0 /* idx */).(*TestServer)
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(b, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`)

	// Create some tables. Specify below in the `backgroundTableName` and
	// benchTableName consts the tables to use for the specific benchmark run.
	sqlRun.Exec(b,
		fmt.Sprintf(`CREATE DATABASE t;
        %s
        %s
        %s
        %s`, tableDefMap["t.a1"], tableDefMap["t.a2"], tableDefMap["t.a3"], tableDefMap["t.a4"],
		))

	fmt.Println("")
	loadBGTableStmt := fmt.Sprintf(`INSERT INTO %s SELECT g, 'foo' FROM generate_series(1,500000) g(g);`, backgroundTableName)
	loadBenchTableStmt := fmt.Sprintf(`INSERT INTO %s SELECT g, 'foo' FROM generate_series(1,100000) g(g);`, benchTableName)
	OltpStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=1;`, benchTableName)
	olapBenchQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s a, %s b, %s c, %s d 
                            WHERE a.k = b.k AND b.k = c.k and c.k = d.k;`,
		benchTableName, benchTableName, benchTableName, benchTableName)
	insStmt := fmt.Sprintf(`INSERT INTO %s VALUES (1, 'foo');`, benchTableName)

	olapBackgroundQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s a;`, backgroundTableName)
	stopper := tc.Stopper()

	benchParams := qosBenchmarkParams{
		stopper:                 stopper,
		gatewayServer:           gatewayServer,
		ctx:                     ctx,
		sqlRun:                  sqlRun, // Runner to use for the main SQL
		backgroundSqlQoSLevel:   BackgroundSqlQoSLevel,
		backgroundSqlStmt:       olapBackgroundQueryStmt, // The specific background SQL to run
		backgroundSqlNumQueries: BackgroundSqlNumQueries, // Adjusts the CPU contention
	}

	printRunInfo(backgroundTableName, benchTableName, loadBGTableStmt,
		loadBenchTableStmt, OltpStmt, olapBenchQueryStmt, insStmt, olapBackgroundQueryStmt)

	// Insert rows into the background and benchmark tables.
	sqlRun.Exec(b, loadBGTableStmt)
	sqlRun.Exec(b, loadBenchTableStmt)

	startBackgroundSQL(b, benchParams)
	// Let the background workload warm up and stabilize.
	time.Sleep(1 * time.Second)

	// Set the QoS level of the main SQL we're benchmarking.
	setQoSStmt, _ := qosSetStmtDict[BenchmarkSqlQoSLevel]
	sqlRun.Exec(b, setQoSStmt)

	// No need to change this, but if there is a lot of variability in runtimes,
	// trying changing numOps to see if issuing many statements together helps.
	const numOps = 1

	// Background SQLs: OLAP     BenchMark SQLs: OLTP
	olapOltp := benchQueryWithQoS(benchParams, numOps, OltpStmt)
	b.Run(`backgroundOlap_OLTP`, func(b *testing.B) {
		olapOltp(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLAP
	olapOlap := benchQueryWithQoS(benchParams, numOps, olapBenchQueryStmt)
	b.Run(`backgroundOlap_OLAP`, func(b *testing.B) {
		olapOlap(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLTP Inserts
	olapOltpDML := benchDMLWithQoS(benchParams, numOps, insStmt)
	b.Run(`backgroundOlap_DML`, func(b *testing.B) {
		olapOltpDML(b)
	})

	stopper.Quiesce(ctx)
	b.ReportAllocs()
}
