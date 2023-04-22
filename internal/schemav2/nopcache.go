package schemav2

type nopCache struct {
}

func (c *nopCache) Put(id string, data []byte) {
}

func (c *nopCache) Get(id string) []byte {
	return nil
}
