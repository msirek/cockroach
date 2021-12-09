// Copyright 2021 The Cockroach Authors.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

package main

import (
	"fmt"
	"io"
)

const lookupJoinerTmpl = "pkg/sql/colexec/colexecjoin/lookupjoiner_tmpl.go"

func genLookupJoiner(inputFileContents string, wr io.Writer) error {
	_, err := fmt.Fprint(wr, inputFileContents)
	return err
}

func init() {
	registerGenerator(genLookupJoiner, "lookupjoiner.eg.go", lookupJoinerTmpl)
}
