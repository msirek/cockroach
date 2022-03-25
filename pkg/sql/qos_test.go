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
	"fmt"
	"testing"

	"github.com/cockroachdb/cockroach/pkg/base"
	"github.com/cockroachdb/cockroach/pkg/keys"
	"github.com/cockroachdb/cockroach/pkg/settings/cluster"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog"
	"github.com/cockroachdb/cockroach/pkg/sql/catalog/desctestutils"
	"github.com/cockroachdb/cockroach/pkg/sql/sem/tree"
	"github.com/cockroachdb/cockroach/pkg/testutils/serverutils"
	"github.com/cockroachdb/cockroach/pkg/testutils/sqlutils"
	"github.com/cockroachdb/cockroach/pkg/util/leaktest"
	"github.com/cockroachdb/cockroach/pkg/util/log"
)

func BenchmarkQoS(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()

	s, sqlDB, _ := serverutils.StartServer(b, base.TestServerArgs{})
	defer s.Stopper().Stop(ctx)

	st := cluster.MakeTestingClusterSettings()
	evalCtx := tree.NewTestingEvalContext(st)
	defer evalCtx.Stop(ctx)

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

	tabDescrs := make([]catalog.TableDescriptor, 0, 10)
	for i := 1; i <= 10; i++ {
		tabDescrs = append(tabDescrs,
			desctestutils.TestingGetPublicTableDescriptor(s.DB(), keys.SystemSQLCodec,
				"t", fmt.Sprintf("a%d", i)))
	}

	const numInsSels = 10
	for i := 0; i < numInsSels; i++ {
		go func() {
			sqlRun.Exec(b,
				`SET default_transaction_quality_of_service=critical;
        insert into t.a1 select g from generate_series(1,100000) g(g);
`)
		}()
	}

	// Run the operation 1000 times.
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
	// This is the slower way to construct a copy of filters.
	b.Run(`BenchmarkQoS`, func(b *testing.B) {
		runBench1(b)
	})
}
