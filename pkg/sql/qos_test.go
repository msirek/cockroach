// Copyright 2022 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package sql

import (
	"context"
	gosql "database/sql"
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/server"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/testcluster"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkQoS1(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	//s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	tc := testcluster.StartTestCluster(b, 1, base.TestClusterArgs{
		ServerArgs: base.TestServerArgs{},
	})
	defer tc.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)
	sqlDB := tc.ServerConn(0)

	gatewayServer := tc.Server(0 /* idx */).(*server.TestServer)
	status := gatewayServer.status
	const numStmts = 32
	var sqlDBs []*gosql.DB
	var sqlRunners []*sqlutils.SQLRunner
	sqlDBs = make([]*gosql.DB, 0, numStmts)
	sqlRunners = make([]*sqlutils.SQLRunner, 0, numStmts)

	for i := 0; i < numStmts; i++ {
		sqlDBs = append(sqlDBs, serverutils.OpenDBConn(
			b, gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
			gatewayServer.Stopper()))
		sqlRunners = append(sqlRunners, sqlutils.MakeSQLRunner(sqlDBs[i]))
	}
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(b,
		`CREATE DATABASE t;
        CREATE TABLE t.a1 (k INT PRIMARY KEY USING HASH WITH BUCKET_COUNT = 256);
        CREATE TABLE t.a2 (k INT);
        CREATE TABLE t.a3 (k INT);
        CREATE TABLE t.a4 (k INT);
        CREATE TABLE t.a5 (k INT);
        CREATE TABLE t.a6 (k INT);
        CREATE TABLE t.a7 (k INT);
        CREATE TABLE t.a8 (k INT);
        CREATE TABLE t.a9 (k INT);
        CREATE TABLE t.a10 (k INT);

`)

	var setStmt string
	critical := false
	if critical {
		setStmt = `SET default_transaction_quality_of_service=critical; `
	} else {
		setStmt = `SET default_transaction_quality_of_service=background; `
	}
	sqlRunners[0].Exec(b, fmt.Sprintf("%s %s", setStmt,
		"insert into t.a3 select g from generate_series(1,100000) g(g);"))

	for i := 0; i < numStmts; i++ {
		go func() {
			idx := i
			//var explainText string
			rows := sqlRunners[idx].Query(b, fmt.Sprintf("%s %s", setStmt,
				`SELECT COUNT(*) FROM t.a3 a, t.a3 b, t.a3 c, t.a3 d WHERE a.k = b.k AND 
					b.k = c.k and c.k = d.k;`))
			defer rows.Close()
			//for rows.Next() {
			//	if err := rows.Scan(&explainText); err != nil {
			//		b.Fatalf("Unexpected error: %v", err)
			//	}
			//	fmt.Println(explainText)
			//}  // msirek-temp\
		}()
	}

	const numOps = 10
	runBench1 := func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				sqlRun.Exec(b,
					`SET default_transaction_quality_of_service=background;
        insert into t.a2 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a3 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a4 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a5 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a6 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
`)
			}
		}
		b.StopTimer()
	}

	runBench2 := func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				sqlRun.Exec(b,
					`SET default_transaction_quality_of_service=critical;
        insert into t.a2 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a3 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a4 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a5 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
        insert into t.a6 VALUES (1),(1),(1),(1),(1),(1),(1),(1),(1),(1);
`)
			}
		}
		b.StopTimer()
	}

	b.Run(`Bench1`, func(b *testing.B) {
		runBench1(b)
	})
	b.Run(`Bench2`, func(b *testing.B) {
		runBench2(b)
	})

	const setStmt3 = `SET default_transaction_quality_of_service=background; `
	runBench3 := func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				rows := sqlRun.Query(b, fmt.Sprintf("%s %s", setStmt3,
					`SELECT COUNT(*) FROM t.a3 a, t.a3 b, t.a3 c, t.a3 d WHERE a.k = b.k AND 
					b.k = c.k and c.k = d.k;`))
				defer rows.Close()
			}
		}
		b.StopTimer()
	}

	const setStmt4 = `SET default_transaction_quality_of_service=critical; `
	runBench4 := func(b *testing.B) {
		b.ResetTimer()
		b.StartTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				rows := sqlRun.Query(b, fmt.Sprintf("%s %s", setStmt4,
					`SELECT COUNT(*) FROM t.a3 a, t.a3 b, t.a3 c, t.a3 d WHERE a.k = b.k AND 
					b.k = c.k and c.k = d.k;`))
				defer rows.Close()
			}
		}
		b.StopTimer()
	}

	b.Run(`Bench3`, func(b *testing.B) {
		runBench3(b)
	})
	b.Run(`Bench4`, func(b *testing.B) {
		runBench4(b)
	})
}
