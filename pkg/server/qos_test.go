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

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/sql/sessiondatapb"
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

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
	stopper               *stop.Stopper
	gatewayServer         *TestServer
	ctx                   context.Context
	sqlRun                *sqlutils.SQLRunner
	backgroundSqlQoSLevel QoSUserLevel
	backgroundSqlStmt     string
	backgroundSqlNumRuns  int
}

func startBackgroundSQL(b *testing.B, params qosBenchmarkParams) {
	_ = params.stopper.RunAsyncTask(params.ctx, "Background Query", func(ctx context.Context) {
		sqlDB := serverutils.OpenDBConn(
			b, params.gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
			params.stopper)
		defer sqlDB.Close()
		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		for j := 0; j < params.backgroundSqlNumRuns; j++ {
			select {
			case <-params.stopper.ShouldQuiesce():
				return
			case <-params.stopper.IsStopped():
				return
			default:
			}
			backgroundSqlSetQoSStmt, _ := qosSetStmtDict[params.backgroundSqlQoSLevel]
			sqlRunner.Exec(b, fmt.Sprintf("%s %s", backgroundSqlSetQoSStmt, params.backgroundSqlStmt))
		}
	},
	)
}

func benchQueryWithQoS(
	params qosBenchmarkParams, numOps int, qoSLevel QoSUserLevel, queryStmt string,
) func(b *testing.B) {
	return func(b *testing.B) {
		// Kick off some SQLs in asynchronous tasks for CPU contention.
		for i := 0; i < params.backgroundSqlNumRuns; i++ {
			startBackgroundSQL(b, params)
		}

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

func BenchmarkQoS1(b *testing.B) {
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
	const tableName = `t.a1`

	insSelStmt := fmt.Sprintf(`insert into %s select g, 'foo' from generate_series(1,50000) g(g);`, tableName)
	sqlRun.Exec(b, insSelStmt)

	olapQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s a, %s b, %s c, %s d WHERE a.k = b.k AND 
                    b.k = c.k and c.k = d.k;`, tableName, tableName, tableName, tableName)
	olapQueryStmt2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s a;`, tableName)
	stopper := tc.Stopper()

	benchParams := qosBenchmarkParams{
		stopper:               stopper,
		gatewayServer:         gatewayServer,
		ctx:                   ctx,
		sqlRun:                sqlRun,         // Runner to use for the main SQL
		backgroundSqlQoSLevel: Background,     // CHANGE THIS and compare runtimes
		backgroundSqlStmt:     olapQueryStmt2, // The specific background SQL to run
		backgroundSqlNumRuns:  1,              // Adjusts the CPU contention
	}

	// CHANGE THIS along with backgroundSqlQoSLevel for comparative benchmarking.
	// Valid levels: Background, Regular, Critical
	const qosLevel = Background
	// Set the QoS level of the main SQL we're benchmarking.
	setQoSStmt, _ := qosSetStmtDict[qosLevel]
	sqlRun.Exec(b, setQoSStmt)

	// Change numOps to see if issuing many statements in a tight loop matters.
	const numOps = 25
	const insStmt = `insert into t.a2 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a3 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a4 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a5 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a6 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);`

	// Background SQLs: OLAP     BenchMark SQLs: OLTP Inserts
	olapOltpDML := benchQueryWithQoS(benchParams, numOps, qosLevel, insStmt)
	b.Run(`backgroundOlap_DML`, func(b *testing.B) {
		olapOltpDML(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLAP
	olapOlap := benchQueryWithQoS(benchParams, numOps, qosLevel, olapQueryStmt)
	b.Run(`backgroundOlap_OLAP`, func(b *testing.B) {
		olapOlap(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLTP
	OltpStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=1;`, tableName)
	olapOltp := benchQueryWithQoS(benchParams, numOps, qosLevel, OltpStmt)
	b.Run(`backgroundOlap_OLTP`, func(b *testing.B) {
		olapOltp(b)
	})

	stopper.Quiesce(ctx)
}
