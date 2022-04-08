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
const BenchmarkSqlQoSLevel = Critical

// Adjusts the CPU contention by specifying the number of simultaneous background
// queries to run.  CHANGE THIS and compare runtimes.
const BackgroundSqlNumQueries = 4

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
) {
	var backgroundSqlQoSLevelString string
	switch BackgroundSqlQoSLevel {
	case Background:
		backgroundSqlQoSLevelString = "Background"
	case Regular:
		backgroundSqlQoSLevelString = "Regular"
	case Critical:
		backgroundSqlQoSLevelString = "Critical"
	}
	var benchmarkSqlQoSLevelString string
	switch BenchmarkSqlQoSLevel {
	case Background:
		benchmarkSqlQoSLevelString = "Background"
	case Regular:
		benchmarkSqlQoSLevelString = "Regular"
	case Critical:
		benchmarkSqlQoSLevelString = "Critical"
	}
	fmt.Println("—————————————————————————————————————————————————————————————————————")
	fmt.Printf("BackgroundSqlQoSLevel: %s     ", backgroundSqlQoSLevelString)
	fmt.Printf("BenchmarkSqlQoSLevel: %s     ", benchmarkSqlQoSLevelString)
	fmt.Printf("BackgroundSqlNumQueries: %d\n", BackgroundSqlNumQueries)
	fmt.Printf("backgroundTableName:   %s        ", backgroundTableName)
	fmt.Printf("benchTableName:       %s\n", benchTableName)
	fmt.Println(loadBGTableStmt)
	fmt.Println(loadBenchTableStmt)
	fmt.Println("—————————————————————————————————————————————————————————————————————")
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
				sqlRunner.Exec(b, params.backgroundSqlStmt)
			}
		},
		)
	}
}

func benchQueryWithQoS(
	params qosBenchmarkParams, numOps int, qoSLevel QoSUserLevel, queryStmt string,
) func(b *testing.B) {
	return func(b *testing.B) {
		// Kick off some SQLs in asynchronous tasks for CPU contention.
		//for i := 0; i < params.backgroundSqlNumQueries; i++ {
		//	startBackgroundSQL(b, params)
		//}  // msirek-temp

		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				params.sqlRun.Exec(b, fmt.Sprintf("%s", queryStmt))
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
	params.SQLMemoryPoolSize = 8 << 30
	tc := serverutils.StartNewTestCluster(b, 1, /* numNodes */
		base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)
	sqlDB := tc.ServerConn(0)
	gatewayServer := tc.Server(0 /* idx */).(*TestServer)
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	// Create some tables. Specific below in the `tableName` const the table to
	// use for the specific benchmark run.
	//
	sqlRun.Exec(b,
		`CREATE DATABASE t;
        CREATE TABLE t.a1 (k INT PRIMARY KEY USING HASH WITH BUCKET_COUNT = 256, v CHAR(3));
        CREATE TABLE t.a2 (k INT PRIMARY KEY, v CHAR(3));
        CREATE TABLE t.a3 (k INT, v CHAR(3));
        CREATE TABLE t.a4 (k INT);
        CREATE TABLE t.a5 (k INT);
        CREATE TABLE t.a6 (k INT);
        CREATE TABLE t.a7 (k INT);
        CREATE TABLE t.a8 (k INT);
        CREATE TABLE t.a9 (k INT);
        CREATE TABLE t.a10 (k INT);

`)

	const backgroundTableName = `t.a2`
	const benchTableName = `t.a3`
	fmt.Println("")
	loadBGTableStmt := fmt.Sprintf(`insert into %s select g, 'foo' from generate_series(1,500000) g(g);`, backgroundTableName)

	loadBenchTableStmt := fmt.Sprintf(`insert into %s select g, 'foo' from generate_series(1,100000) g(g);`, benchTableName)
	printRunInfo(backgroundTableName, benchTableName, loadBGTableStmt, loadBenchTableStmt)

	sqlRun.Exec(b, loadBGTableStmt)
	sqlRun.Exec(b, loadBenchTableStmt)

	//olapQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s a, %s b, %s c, %s d WHERE a.k = b.k AND
	//                  b.k = c.k and c.k = d.k;`, backgroundTableName, backgroundTableName,
	//	backgroundTableName, backgroundTableName)
	olapQueryStmt2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s a;`, backgroundTableName)
	stopper := tc.Stopper()

	benchParams := qosBenchmarkParams{
		stopper:                 stopper,
		gatewayServer:           gatewayServer,
		ctx:                     ctx,
		sqlRun:                  sqlRun, // Runner to use for the main SQL
		backgroundSqlQoSLevel:   BackgroundSqlQoSLevel,
		backgroundSqlStmt:       olapQueryStmt2,          // The specific background SQL to run
		backgroundSqlNumQueries: BackgroundSqlNumQueries, // Adjusts the CPU contention
	}

	startBackgroundSQL(b, benchParams)
	// Let the workload warm up and stabilize.
	time.Sleep(1 * time.Second)

	// Set the QoS level of the main SQL we're benchmarking.
	setQoSStmt, _ := qosSetStmtDict[BenchmarkSqlQoSLevel]
	sqlRun.Exec(b, setQoSStmt)

	// Change numOps to see if issuing many statements in a tight loop matters.
	const numOps = 1
	const insStmt = `insert into t.a2 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a3 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a4 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a5 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a6 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);`

	// Background SQLs: OLAP     BenchMark SQLs: OLTP Inserts
	olapOltpDML := benchQueryWithQoS(benchParams, numOps, BenchmarkSqlQoSLevel, insStmt)
	b.Run(`backgroundOlap_DML`, func(b *testing.B) {
		olapOltpDML(b)
	})

	olapBenchQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s a, %s b, %s c, %s d WHERE a.k = b.k AND 
                    b.k = c.k and c.k = d.k;`, benchTableName, benchTableName,
		benchTableName, benchTableName)
	//olapBenchQueryStmt2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s a;`, benchTableName)

	// Background SQLs: OLAP     BenchMark SQLs: OLAP
	olapOlap := benchQueryWithQoS(benchParams, numOps, BenchmarkSqlQoSLevel, olapBenchQueryStmt)
	b.Run(`backgroundOlap_OLAP`, func(b *testing.B) {
		olapOlap(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLTP
	OltpStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=1;`, benchTableName)
	olapOltp := benchQueryWithQoS(benchParams, numOps, BenchmarkSqlQoSLevel, OltpStmt)
	b.Run(`backgroundOlap_OLTP`, func(b *testing.B) {
		olapOltp(b)
	})

	stopper.Quiesce(ctx)
}
