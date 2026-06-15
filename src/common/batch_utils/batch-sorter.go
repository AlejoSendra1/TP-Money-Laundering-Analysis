package batch_utils

import "sort"

func SortBatch[T any](records []T, less func(T, T) bool) {
	sort.Slice(records, func(i, j int) bool {
		return less(records[i], records[j])
	})
}
