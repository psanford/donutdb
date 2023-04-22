package sectorcache

type CacheV2 interface {
	Get(string) []byte
	Put(string, []byte)
}
