package appdash

import "testing"

func TestInsertSpan(t *testing.T) {
	cfg := MySQLConfig{}
	store := NewMySQLStore(cfg)
	err := store.Collect(SpanID{1, 1, 0}, Annotation{Key: "k", Value: []byte("v")}, Annotation{Key: "k", Value: []byte("v")})
	if err != nil {
		t.Errorf("MySQLStoreCollect Err:%v", err)
	}
}
