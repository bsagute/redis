package main

import (
	"context"       // for Redis context
	"crypto/rand"   // for secure randomness
	"encoding/json" // for JSON marshaling
	"fmt"           // for formatted I/O
	"log"           // for fatal logging
	"math/big"      // for large random ints
	"strings"       // for parsing INFO output
	"time"          // for timing operations

	"github.com/go-redis/redis/v8" // Redis client
	"github.com/google/uuid"       // UUIDs for unique keys
)

var ctx = context.Background()

// sampleCounts: how many records to insert & fetch in each test
var sampleCounts = []int{10, 100, 1_000, 10_000, 100_000}

// Record is the data we store in Redis
type Record struct {
	ID     string  `json:"id"`     // UUID (also used as key suffix)
	Name   string  `json:"name"`   // random name
	Email  string  `json:"email"`  // random email
	Amount float64 `json:"amount"` // random amount
}

func main() {
	// 1) Connect to Redis (localhost:6379, DB0)
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
	defer rdb.Close()

	// 2) Track keys for final cleanup
	var insertedKeys []string

	// 3) Print table header
	fmt.Println("Redis: pipeline vs Lua for GET + HGET")
	fmt.Println("Count  | ΔMem (MB) | Direct        | Pipeline      | Lua")
	fmt.Println("-------+-----------+---------------+---------------+--------------")

	// prevKeys holds keys from previous iteration to delete before next test
	var prevKeys []string

	// 4) Loop through each test size
	for _, n := range sampleCounts {
		// 4a) Remove keys from previous test only
		if len(prevKeys) > 0 {
			if err := deleteInsertedKeys(rdb, prevKeys); err != nil {
				log.Fatalf("cleanup before test for %d failed: %v", n, err)
			}
		}
		prevKeys = prevKeys[:0] // reset for this iteration

		// 4b) Measure memory before inserting
		beforeBytes, _ := getMemory(rdb)

		// 4c) Insert n records, tracking each key under "bench:<UUID>"
		keys := make([]string, 0, n)
		for i := 0; i < n; i++ {
			rec := generateRecord()
			key := "bench:" + rec.ID

			// a) Store whole record as JSON string
			data, _ := json.Marshal(rec)
			if err := rdb.Set(ctx, key, data, 0).Err(); err != nil {
				log.Fatalf("SET failed: %v", err)
			}
			// b) Store only the email in a hash field
			if err := rdb.HSet(ctx, key, "email", rec.Email).Err(); err != nil {
				log.Fatalf("HSET failed: %v", err)
			}

			keys = append(keys, key)
			prevKeys = append(prevKeys, key)
			insertedKeys = append(insertedKeys, key)
		}

		// 4d) Measure memory after inserting & compute delta
		afterBytes, _ := getMemory(rdb)
		deltaMB := float64(afterBytes-beforeBytes) / (1024 * 1024)

		// 4e) Direct fetch: n × (GET + HGET)
		t0 := time.Now()
		for _, key := range keys {
			if _, err := rdb.Get(ctx, key).Result(); err != nil {
				log.Fatalf("GET failed: %v", err)
			}
			if _, err := rdb.HGet(ctx, key, "email").Result(); err != nil {
				log.Fatalf("HGET failed: %v", err)
			}
		}
		durDirect := time.Since(t0)

		// 4f) Pipeline fetch: batch GET + HGET
		t1 := time.Now()
		pipe := rdb.Pipeline()
		for _, key := range keys {
			pipe.Get(ctx, key)
			pipe.HGet(ctx, key, "email")
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("Pipeline exec failed: %v", err)
		}
		durPipe := time.Since(t1)

		// 4g) Lua script fetch: server‐side atomic GET+HGET
		lua := redis.NewScript(`
            local res = {}
            for _, k in ipairs(ARGV) do
                local v = redis.call("GET", k)
                local e = redis.call("HGET", k, "email")
                table.insert(res, {v, e})
            end
            return res
        `)
		t2 := time.Now()
		if _, err := lua.Run(ctx, rdb, nil, keys).Result(); err != nil {
			log.Fatalf("Lua script failed: %v", err)
		}
		durLua := time.Since(t2)

		// 4h) Print results row
		fmt.Printf("%6d | %+7.2f   | %13v | %13v | %12v\n",
			n, deltaMB, durDirect, durPipe, durLua)
	}

	// 5) Final cleanup: delete exactly our "bench:" keys
	if err := deleteInsertedKeys(rdb, insertedKeys); err != nil {
		log.Fatalf("final cleanup failed: %v", err)
	}
	fmt.Println("✅ Cleanup complete: only bench:* keys removed")
}

// getMemory reads Redis INFO memory and returns bytes & human string.
func getMemory(rdb *redis.Client) (bytes int64, human string) {
	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		log.Fatalf("INFO memory failed: %v", err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			fmt.Sscanf(line, "used_memory:%d", &bytes)
		}
		if strings.HasPrefix(line, "used_memory_human:") {
			parts := strings.SplitN(line, ":", 2)
			human = strings.TrimSpace(parts[1])
		}
	}
	return
}

// deleteInsertedKeys deletes only the provided list of keys, in batches.
func deleteInsertedKeys(rdb *redis.Client, keys []string) error {
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		if err := rdb.Del(ctx, keys[i:end]...).Err(); err != nil {
			return fmt.Errorf("del batch %d–%d: %w", i, end, err)
		}
	}
	return nil
}

// generateRecord creates a Record with random data.
func generateRecord() Record {
	return Record{
		ID:     uuid.New().String(),                       // unique id
		Name:   randStr(6),                                // random name
		Email:  fmt.Sprintf("%s@example.com", randStr(8)), // random email
		Amount: float64(randInt(100, 99999)),              // random amount
	}
}

// randStr returns a random lowercase string of length n.
func randStr(n int) string {
	letters := "abcdefghijklmnopqrstuvwxyz"
	buf := make([]byte, n)
	for i := range buf {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		buf[i] = letters[idx.Int64()]
	}
	return string(buf)
}

// randInt returns a random integer [min, max).
func randInt(min, max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(n.Int64()) + min
}
