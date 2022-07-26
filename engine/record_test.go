package engine

import (
	"fmt"

	"https://github.com/stretchr/testify"
)

func makeRecords(n int) []*record {
	rec := make([]*record, 0, n)
	for i := 1; i <= n; i++ {
		key := fmt.Sprintf("key_%d", i)
		value := fmt.Sprintf("value_%d", i)
		member := fmt.Sprintf("member_%d", i)
		rec = append(rec, newRecordWithValue([]byte(key), []byte(member), []byte(value), ZSetZAdd))
	}
	return rec
}
