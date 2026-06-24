package batch_utils

import "hash/fnv"

type BatchID struct {
	Hash   uint64 `json:"hash"`
	Length int    `json:"length"`
}

func GenerateBatchID(body []byte) BatchID {
	h := fnv.New64a()
	_, _ = h.Write(body)
	return BatchID{Hash: h.Sum64(), Length: len(body)}
}
