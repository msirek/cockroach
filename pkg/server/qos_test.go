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
	"github.com/cockroachdb/cockroach/pkg/sql/tests"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
	"github.com/cockroachdb/cockroach/pkg/util/stop"
)

func startBackgroundSQL(
	b *testing.B,
	stopper *stop.Stopper,
	gatewayServer *TestServer,
	ctx context.Context,
	setQoSStmt string,
	queryStmt string,
	numRuns int,
) {
	_ = stopper.RunAsyncTask(ctx, "Background Query", func(ctx context.Context) {
		sqlDB := serverutils.OpenDBConn(
			b, gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
			stopper)
		defer sqlDB.Close()
		sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
		for j := 0; j < numRuns; j++ {
			select {
			case <-stopper.ShouldQuiesce():
				return
			case <-stopper.IsStopped():
				return
			default:
			}
			sqlRunner.Exec(b, fmt.Sprintf("%s %s", setQoSStmt, queryStmt))
		}
	},
	)
}

func benchQueryWithQoS(
	numOps int, sqlRun *sqlutils.SQLRunner, setQoSStmt string, queryStmt string,
) func(b *testing.B) {
	return func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				sqlRun.Exec(b, fmt.Sprintf("%s %s", setQoSStmt, queryStmt))
			}
		}
		b.StopTimer()
	}
}

func BenchmarkQoS1(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	//s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	//tc := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{
	//	ServerArgs: base.TestServerArgs{},
	//})
	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	params.SQLMemoryPoolSize = 8 << 30
	tc := serverutils.StartNewTestCluster(b, 1, /* numNodes */
		base.TestClusterArgs{ServerArgs: params})
	defer tc.Stopper().Stop(ctx)

	//st := cluster.MakeTestingClusterSettings()
	//evalCtx := tree.NewTestingEvalContext(st)
	//defer evalCtx.Stop(ctx)
	sqlDB := tc.ServerConn(0)

	gatewayServer := tc.Server(0 /* idx */).(*TestServer)

	//var sqlDBs []*gosql.DB
	//var sqlRunners []*sqlutils.SQLRunner
	//sqlDBs = make([]*gosql.DB, 0, numStmts)
	//sqlRunners = make([]*sqlutils.SQLRunner, 0, numStmts)
	//
	//for i := 0; i < numStmts; i++ {
	//	sqlDBs = append(sqlDBs, serverutils.OpenDBConn(
	//		b, gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
	//		gatewayServer.Stopper()))
	//	sqlRunners = append(sqlRunners, sqlutils.MakeSQLRunner(sqlDBs[i]))
	//}
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(b,
		`CREATE DATABASE t;
        CREATE TABLE t.a1 (k INT PRIMARY KEY USING HASH WITH BUCKET_COUNT = 256, v CHAR(30000));
        CREATE TABLE t.a2 (k INT PRIMARY KEY, v CHAR(30000));
        CREATE TABLE t.a3 (k INT, v CHAR(30000));
        CREATE TABLE t.a4 (k INT);
        CREATE TABLE t.a5 (k INT);
        CREATE TABLE t.a6 (k INT);
        CREATE TABLE t.a7 (k INT);
        CREATE TABLE t.a8 (k INT);
        CREATE TABLE t.a9 (k INT);
        CREATE TABLE t.a10 (k INT);

`)

	var setQoSStmt string
	critical := false
	if critical {
		setQoSStmt = `SET default_transaction_quality_of_service=regular; `
	} else {
		setQoSStmt = `SET default_transaction_quality_of_service=background; `
	}
	const tableName = `t.a1`
	insSelStmt := fmt.Sprintf(`insert into %s select g, 'foo' from generate_series(1,100000) g(g);`, tableName)
	sqlRun.Exec(b, fmt.Sprintf("%s %s", setQoSStmt, insSelStmt))

	////go func() {
	//for i := 0; i < numStmts; i++ {
	//	//var explainText string
	//	sqlRunners[i].Exec(b, fmt.Sprintf("%s %s", setStmt,
	//		`INSERT INTO t.a10 SELECT COUNT(*) FROM t.a3 a, t.a3 b, t.a3 c, t.a3 d WHERE a.k = b.k AND
	//				b.k = c.k and c.k = d.k;`))
	//	// defer rows.Close()
	//	//for rows.Next() {
	//	//	if err := rows.Scan(&explainText); err != nil {
	//	//		b.Fatalf("Unexpected error: %v", err)
	//	//	}
	//	//	fmt.Println(explainText)
	//	//}  // msirek-temp\
	//}
	////}()
	olapQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s a, %s b, %s c, %s d WHERE a.k = b.k AND 
                    b.k = c.k and c.k = d.k;`, tableName, tableName, tableName, tableName)
	//olapQueryStmt2 := fmt.Sprintf(`SELECT COUNT(*) FROM %s a;`, tableName)
	stopper := tc.Stopper()
	// Start a bunch of asynchronous SQLs to provide workload contention with
	// our benchmark queries.
	const numStmts = 32
	for i := 0; i < numStmts; i++ {
		startBackgroundSQL(
			b, stopper, gatewayServer, ctx, setQoSStmt, olapQueryStmt, 3)
	}
	// Make sure the background queries have started.
	time.Sleep(10 * time.Millisecond)
	const setBackgroundQoSStmt = `SET default_transaction_quality_of_service=background;`
	const setCriticalQoSStmt = `SET default_transaction_quality_of_service=critical;`

	const numOps = 10
	const insStmt = `insert into t.a2 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a3 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a4 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a5 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a6 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);`

	runBench1 := benchQueryWithQoS(numOps, sqlRun, setBackgroundQoSStmt, insStmt)
	runBench2 := benchQueryWithQoS(numOps, sqlRun, setCriticalQoSStmt, insStmt)

	b.Run(`Bench1`, func(b *testing.B) {
		runBench1(b)
	})
	b.Run(`Bench2`, func(b *testing.B) {
		runBench2(b)
	})

	const numOps2 = 1
	runBench3 := benchQueryWithQoS(numOps2, sqlRun, setBackgroundQoSStmt, olapQueryStmt)
	runBench4 := benchQueryWithQoS(numOps2, sqlRun, setCriticalQoSStmt, olapQueryStmt)

	b.Run(`Bench3`, func(b *testing.B) {
		runBench3(b)
	})
	b.Run(`Bench4`, func(b *testing.B) {
		runBench4(b)
	})

	OltpStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=1;`, tableName)
	runBench5 := benchQueryWithQoS(numOps2, sqlRun, setBackgroundQoSStmt, OltpStmt)
	runBench6 := benchQueryWithQoS(numOps2, sqlRun, setCriticalQoSStmt, OltpStmt)

	b.Run(`Bench5`, func(b *testing.B) {
		runBench5(b)
	})

	b.Run(`Bench6`, func(b *testing.B) {
		runBench6(b)
	})

	stopper.Quiesce(ctx)
}
