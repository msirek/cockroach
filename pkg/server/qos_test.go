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
	"sync"
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

// The Quality of Service level to use for the SQLs we are benchmarking.
// Valid values: Background, Regular, Critical
const BenchmarkSqlQoSLevel = Regular

// Adjusts the CPU contention by specifying the number of simultaneous background
// queries to run.  CHANGE THIS and compare runtimes.
const BackgroundSqlNumQueries = 16

// Define which tables to use for the background queries and benchmark queries.
const backgroundTableName = `t.a4`
const benchTableName = `t.a3`

// Define the OLAP background query statement.
func olapBackgroundQuery() string {
	olapBackgroundQueryStmt := fmt.Sprintf(`SELECT MAX(v) FROM %s;`, backgroundTableName) // msirek-temp
	//olapBackgroundQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=1;`, backgroundTableName) // msirek-temp
	//olapBackgroundQueryStmt := fmt.Sprintf(`SELECT SUM(a.k+b.k+c.k+d.k) FROM %s a
	//                    INNER HASH JOIN %s b ON a.k = b.k
	//                    INNER HASH JOIN %s c ON b.k = c.k
	//                    INNER HASH JOIN %s d ON c.k = d.k;`,
	//	backgroundTableName, backgroundTableName, backgroundTableName, backgroundTableName)
	return olapBackgroundQueryStmt
}

var tableDefMap = map[string]string{
	"t.a1": `CREATE TABLE t.a1 (k INT PRIMARY KEY USING HASH WITH
                                              BUCKET_COUNT = 256, v CHAR(3));`,
	"t.a2": `CREATE TABLE t.a2 (k INT PRIMARY KEY, v CHAR(3));`,
	"t.a3": `CREATE TABLE t.a3 (k INT PRIMARY KEY, v CHAR(3));`,
	"t.a4": `CREATE TABLE t.a4 (k INT, v CHAR(90000));`,
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
	stopper       *stop.Stopper
	gatewayServer *TestServer
	ctx           context.Context

	// Runner to use for the main SQL
	sqlRun *sqlutils.SQLRunner

	// The specific background SQL to run
	backgroundSqlStmt string

	// Adjusts the CPU contention by controlling the number of parallel queries
	backgroundSqlNumQueries int

	setQoSStatements []chan string
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
	fmt.Println("———————————————————————————————————————————————————————————————————————————————")
	fmt.Printf("Background SQLs Table:   %s\n", tableDefMap[backgroundTableName])
	fmt.Printf("Benchmark  SQLs Table:   %s\n", tableDefMap[benchTableName])
	fmt.Println()
	fmt.Println(loadBGTableStmt)
	fmt.Println(loadBenchTableStmt)
	fmt.Println()
	fmt.Printf("Background SQL: %s\n", olapBackgroundQueryStmt)
	fmt.Printf("Number of instances of background SQL running in parallel: %d\n",
		BackgroundSqlNumQueries)
	fmt.Println()
	fmt.Println("BenchMark Queries")
	fmt.Printf("OLTP Query: %s\n", oltpBenchQuery)
	fmt.Printf("OLAP Query: %s\n", olapBenchQuery)
	fmt.Printf("OLTP DML  : %s\n", dmlBenchStmt)
	fmt.Println("———————————————————————————————————————————————————————————————————————————————")
}

func startBackgroundSQL(b *testing.B, params *qosBenchmarkParams) {
	for j := 0; j < params.backgroundSqlNumQueries; j++ {
		instanceNumber := j
		//k := 1  // msirek-temp
		_ = params.stopper.RunAsyncTask(params.ctx, "Background Query", func(ctx context.Context) {
			sqlDB := serverutils.OpenDBConn(
				b, params.gatewayServer.ServingSQLAddr(), "" /* useDatabase */, true, /* insecure */
				params.stopper)
			defer sqlDB.Close()
			sqlRunner := sqlutils.MakeSQLRunner(sqlDB)
			for {
				select {
				case <-params.stopper.ShouldQuiesce():
					return
				case <-params.stopper.IsStopped():
					return
				default:
				}
				select {
				case setStatement := <-params.setQoSStatements[instanceNumber]:
					// Set a new QoSLevel if instructed.
					//if instanceNumber == 0 { // msirek-temp
					fmt.Printf("Setting new QoS level for background SQL task %d:  ", instanceNumber)
					fmt.Println(setStatement)
					//}
					sqlRunner.Exec(b, setStatement)
				default:
				}
				//olapBackgroundQueryStmt := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=%d;`, backgroundTableName, k) // msirek-temp
				//rows := sqlRunner.Query(b, olapBackgroundQueryStmt)
				rows := sqlRunner.Query(b, params.backgroundSqlStmt) // msirek-temp
				rows.Close()
				//k++
				//if k > 500000 {
				//	k = 1
				//}
			}
		},
		)
	}
}

func benchQueryWithQoS(
	params *qosBenchmarkParams, numOps int, queryStmt string,
) func(b *testing.B) {
	return func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				rows := params.sqlRun.Query(b, queryStmt)
				if !rows.Next() {
					panic("oh no!") // msirek-temp
				}
				rows.Close()
			}
		}
		b.StopTimer()
	}
}

func benchDMLWithQoS(params *qosBenchmarkParams, numOps int, dmlStmt string) func(b *testing.B) {
	return func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			for j := 0; j < numOps; j++ {
				params.sqlRun.Exec(b, dmlStmt)
			}
		}
		b.StopTimer()
	}
}

func setBackgroundSqlQoS(level QoSUserLevel, params *qosBenchmarkParams) {
	var wg sync.WaitGroup
	for i := 0; i < BackgroundSqlNumQueries; i++ {
		idx := i
		wg.Add(1)
		go func(wg *sync.WaitGroup) {
			defer wg.Done()
			params.setQoSStatements[idx] <- qosSetStmtDict[level]
		}(&wg)
	}
	wg.Wait()
}

func runGC() {
	//// Start memory with a clean slate.
	//runtime.GC()  // msirek-temp
	//
	//// Give GC some time to complete.
	//time.Sleep(5 * time.Second)
}

func setupCluster(
	b *testing.B,
) (
	tc serverutils.TestClusterInterface,
	benchParams *qosBenchmarkParams,
	oltpBenchQueryStmt string,
	olapBenchQueryStmt string,
	oltpBenchDmlStmt string,
) {
	ctx := context.Background()
	params, _ := tests.CreateTestServerParams()
	params.Insecure = true
	params.SQLMemoryPoolSize = 8 << 30
	params.TempStorageConfig =
		base.DefaultTestTempStorageConfigWithSize(cluster.MakeTestingClusterSettings(), 1<<30)
	tc = serverutils.StartNewTestCluster(b, 1, /* numNodes */
		base.TestClusterArgs{ServerArgs: params})

	sqlDB := tc.ServerConn(0)
	gatewayServer := tc.Server(0 /* idx */).(*TestServer)
	sqlRun := sqlutils.MakeSQLRunner(sqlDB)
	sqlRun.Exec(b, `SET CLUSTER SETTING sql.stats.automatic_collection.enabled = false;`)
	sqlRun.Exec(b, `SET CLUSTER SETTING admission.kv.enabled = true;`)
	sqlRun.Exec(b, `SET CLUSTER SETTING admission.sql_kv_response.enabled = true;`)
	sqlRun.Exec(b, `SET CLUSTER SETTING admission.sql_sql_response.enabled = true;`)

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
	loadBGTableStmt := fmt.Sprintf(
		`INSERT INTO %s SELECT CAST(g/10 AS int), 'foo' FROM generate_series(1,500000) g(g);`, backgroundTableName)
	loadBenchTableStmt := fmt.Sprintf(
		`INSERT INTO %s SELECT g, 'foo' FROM generate_series(1,100000) g(g);`, benchTableName)
	oltpBenchQueryStmt = fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE k=1;`, benchTableName)
	//prepareStmt := fmt.Sprintf(`PREPARE s1 AS SELECT COUNT(*) FROM %s WHERE k=1;`, benchTableName)
	//sqlRun.Exec(b, prepareStmt) // msirek-temp

	//oltpBenchQueryStmt = "EXECUTE s1"
	olapBenchQueryStmt = fmt.Sprintf(`SELECT SUM(a.k+b.k+c.k+d.k) FROM %s a 
                       INNER HASH JOIN %s b ON a.k = b.k 
                       INNER HASH JOIN %s c ON b.k = c.k 
                       INNER HASH JOIN %s d ON c.k = d.k;`,
		benchTableName, benchTableName, benchTableName, benchTableName)
	oltpBenchDmlStmt = fmt.Sprintf(`INSERT INTO %s SELECT MAX(k)+1 from %s;`,
		benchTableName, benchTableName)

	stopper := tc.Stopper()

	olapBackgroundQueryStmt := olapBackgroundQuery()
	benchParams = &qosBenchmarkParams{
		stopper:                 stopper,
		gatewayServer:           gatewayServer,
		ctx:                     ctx,
		sqlRun:                  sqlRun,
		backgroundSqlStmt:       olapBackgroundQueryStmt,
		backgroundSqlNumQueries: BackgroundSqlNumQueries,
		setQoSStatements:        make([]chan string, BackgroundSqlNumQueries),
	}
	for i := 0; i < BackgroundSqlNumQueries; i++ {
		benchParams.setQoSStatements[i] = make(chan string)
	}

	printRunInfo(backgroundTableName, benchTableName, loadBGTableStmt,
		loadBenchTableStmt, oltpBenchQueryStmt, olapBenchQueryStmt, oltpBenchDmlStmt, olapBackgroundQueryStmt)

	// Insert rows into the background and benchmark tables.
	sqlRun.Exec(b, loadBGTableStmt)
	sqlRun.Exec(b, loadBenchTableStmt)

	// Disable garbage collection during the test so there is no effect on CPU
	// from this operation kicking in.
	//debug.SetGCPercent(-1)   // msirek-temp

	// This runs some background SQL over and over again while the test runs.
	startBackgroundSQL(b, benchParams)

	// Let the background workload warm up and stabilize.
	time.Sleep(1 * time.Second)

	// Set the QoS level of the main SQL we're benchmarking.
	setQoSStmt, _ := qosSetStmtDict[BenchmarkSqlQoSLevel]
	sqlRun.Exec(b, setQoSStmt)

	return tc, benchParams, oltpBenchQueryStmt, olapBenchQueryStmt, oltpBenchDmlStmt
}

// Test varying the QoS level of the background SQLs while holding the QOS level
// of the benchmark SQLs constant.
func BenchmarkVaryBackgroundQoS(b *testing.B) {
	defer leaktest.AfterTest(b)()
	defer log.Scope(b).Close(b)
	ctx := context.Background()
	tc, benchParams, oltpBenchQueryStmt, olapBenchQueryStmt, oltpBenchDmlStmt := setupCluster(b)
	stopper := tc.Stopper()
	defer stopper.Stop(ctx)

	// No need to change this, but if there is a lot of variability in runtimes,
	// trying changing numOps to see if issuing many statements together helps.
	const numOps = 1

	// Run each benchmark SQL, first with background SQLs at regular QoS, then
	// with background SQLs at background QoS.

	// Background SQLs: OLAP     BenchMark SQLs: OLTP
	firstTime1 := true
	olapOltp1 := func() func(b *testing.B) {
		if firstTime1 {
			firstTime1 = false
			setBackgroundSqlQoS(Regular, benchParams)
			runGC()
		}
		return benchQueryWithQoS(benchParams, numOps, oltpBenchQueryStmt)
	}()

	b.Run(`BackgroundOlap_OLTP`, func(b *testing.B) {
		olapOltp1(b)
	})

	firstTime2 := true
	olapOltp2 := func() func(b *testing.B) {
		if firstTime2 {
			firstTime2 = false
			setBackgroundSqlQoS(Background, benchParams)
			runGC()
		}
		return benchQueryWithQoS(benchParams, numOps, oltpBenchQueryStmt)
	}()

	b.Run(`BackgroundOlap_OLTP_2`, func(b *testing.B) {
		olapOltp2(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLAP
	firstTime3 := true
	olapOlap1 := func() func(b *testing.B) {
		if firstTime3 {
			firstTime3 = false
			setBackgroundSqlQoS(Regular, benchParams)
			runGC()
		}
		return benchQueryWithQoS(benchParams, numOps, olapBenchQueryStmt)
	}()

	b.Run(`BackgroundOlap_OLAP`, func(b *testing.B) {
		olapOlap1(b)
	})

	firstTime4 := true
	olapOlap2 := func() func(b *testing.B) {
		if firstTime4 {
			firstTime4 = false
			setBackgroundSqlQoS(Background, benchParams)
			runGC()
		}
		return benchQueryWithQoS(benchParams, numOps, olapBenchQueryStmt)
	}()

	b.Run(`BackgroundOlap_OLAP_2`, func(b *testing.B) {
		olapOlap2(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLTP DML (inserts)
	firstTime5 := true
	olapOltpDML1 := func() func(b *testing.B) {
		if firstTime5 {
			firstTime5 = false
			setBackgroundSqlQoS(Regular, benchParams)
			runGC()
		}
		return benchDMLWithQoS(benchParams, numOps, oltpBenchDmlStmt)
	}()

	b.Run(`BackgroundOlap_DML`, func(b *testing.B) {
		olapOltpDML1(b)
	})

	// Background SQLs: OLAP     BenchMark SQLs: OLTP DML (inserts)
	firstTime6 := true
	olapOltpDML2 := func() func(b *testing.B) {
		if firstTime6 {
			firstTime6 = false
			setBackgroundSqlQoS(Background, benchParams)
			runGC()
		}
		return benchDMLWithQoS(benchParams, numOps, oltpBenchDmlStmt)
	}()

	b.Run(`BackgroundOlap_DML_2`, func(b *testing.B) {
		olapOltpDML2(b)
	})

	stopper.Quiesce(ctx)
	b.ReportAllocs()
}
