package main

import (
	"context"       // for passing context to Redis
	"crypto/rand"   // for secure random numbers
	"encoding/json" // for marshaling Record structs
	"fmt"           // for formatted I/O
	"log"           // for logging fatal errors
	"math/big"      // for large random-int ranges
	"strings"       // for parsing INFO output
	"time"          // for measuring durations

	"github.com/go-redis/redis/v8" // Redis client
	"github.com/google/uuid"       // for generating UUIDs
)

var ctx = context.Background()

// sampleCounts defines the sizes of data sets to benchmark.
var sampleCounts = []int{10, 100, 1_000, 10_000, 100_000}

// Record is the data structure we'll store in Redis.
type Record struct {
	ID     string  `json:"id"`     // UUID used as part of the key
	Name   string  `json:"name"`   // random 6-letter name
	Email  string  `json:"email"`  // random email
	Amount float64 `json:"amount"` // random float amount
}

func main() {
	// 1) Connect to Redis
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
		DB:   0,
	})
	defer rdb.Close()

	// Track all keys we insert, so cleanup can delete exactly them
	var insertedKeys []string

	// Print table header
	fmt.Println("Redis: pipeline vs Lua for GET + HGET")
	fmt.Println("Count   | ΔMem (MB) | Direct Fetch   | Pipeline Fetch | Lua Fetch")
	fmt.Println("--------+-----------+----------------+----------------+-----------")

	// 2) Loop through each test size
	for _, n := range sampleCounts {
		// a) Flush DB before each run to isolate tests
		if err := rdb.FlushDB(ctx).Err(); err != nil {
			log.Fatalf("FLUSHDB failed: %v", err)
		}

		// b) Measure memory before insertion
		beforeBytes, _ := getMemory(rdb)

		// c) Insert n records under two distinct keys per record:
		//    - "bench:json:<UUID>" for SET/GET
		//    - "bench:hash:<UUID>" for HSET/HGET
		jsonKeys := make([]string, n)
		hashKeys := make([]string, n)
		for i := 0; i < n; i++ {
			rec := generateRecord()
			jsonKey := "bench:json:" + rec.ID
			hashKey := "bench:hash:" + rec.ID

			// Store full JSON under jsonKey
			data, _ := json.Marshal(rec)
			if err := rdb.Set(ctx, jsonKey, data, 0).Err(); err != nil {
				log.Fatalf("SET failed for key %s: %v", jsonKey, err)
			}
			// Store only the email under hashKey
			if err := rdb.HSet(ctx, hashKey, "email", rec.Email).Err(); err != nil {
				log.Fatalf("HSET failed for key %s: %v", hashKey, err)
			}

			// Track keys for fetch and cleanup
			jsonKeys[i] = jsonKey
			hashKeys[i] = hashKey
			insertedKeys = append(insertedKeys, jsonKey, hashKey)
		}

		// d) Measure memory after insertion and compute delta
		afterBytes, _ := getMemory(rdb)
		deltaMB := float64(afterBytes-beforeBytes) / 1024.0 / 1024.0

		// e) Direct fetch: n × (GET + HGET)
		t0 := time.Now()
		for i := 0; i < n; i++ {
			if _, err := rdb.Get(ctx, jsonKeys[i]).Result(); err != nil {
				log.Fatalf("Direct GET failed: %v", err)
			}
			if _, err := rdb.HGet(ctx, hashKeys[i], "email").Result(); err != nil {
				log.Fatalf("Direct HGET failed: %v", err)
			}
		}
		durDirect := time.Since(t0)

		// f) Pipeline fetch: batch GET + HGET in a single round-trip
		t1 := time.Now()
		pipe := rdb.Pipeline()
		for i := 0; i < n; i++ {
			pipe.Get(ctx, jsonKeys[i])
			pipe.HGet(ctx, hashKeys[i], "email")
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("Pipeline exec failed: %v", err)
		}
		durPipe := time.Since(t1)

		// g) Lua fetch: server-side atomic GET + HGET
		luaScript := redis.NewScript(`
            local res = {}
            for i=1,#KEYS do
                local v = redis.call("GET", KEYS[i])
                local e = redis.call("HGET", ARGV[i], "email")
                table.insert(res, {v, e})
            end
            return res
        `)
		t2 := time.Now()
		if _, err := luaScript.Run(ctx, rdb, jsonKeys, hashKeys).Result(); err != nil {
			log.Fatalf("Lua script failed: %v", err)
		}
		durLua := time.Since(t2)

		// h) Print results for this batch size
		fmt.Printf("%6d | %+9.2f | %14v | %14v | %10v\n",
			n, deltaMB, durDirect, durPipe, durLua,
		)
	}

	// 3) Final cleanup: delete exactly the keys we inserted (no others)
	if err := deleteInsertedKeys(rdb, insertedKeys); err != nil {
		log.Fatalf("Final cleanup failed: %v", err)
	}
	fmt.Println("✅ Cleanup complete: only bench:* keys removed")
}

// getMemory returns Redis's used_memory (bytes) and used_memory_human.
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

// deleteInsertedKeys deletes exactly the given keys in batches,
// ensuring no other keys in Redis are touched.
func deleteInsertedKeys(rdb *redis.Client, keys []string) error {
	const batchSize = 1000
	for i := 0; i < len(keys); i += batchSize {
		end := i + batchSize
		if end > len(keys) {
			end = len(keys)
		}
		if err := rdb.Del(ctx, keys[i:end]...).Err(); err != nil {
			return fmt.Errorf("failed deleting keys %d–%d: %w", i, end, err)
		}
	}
	return nil
}

// generateRecord creates a random Record for testing.
func generateRecord() Record {
	return Record{
		ID:     uuid.New().String(),
		Name:   randStr(6),
		Email:  fmt.Sprintf("%s@example.com", randStr(8)),
		Amount: float64(randInt(100, 99999)),
	}
}

// randStr returns a random string of lowercase letters of length n.
func randStr(n int) string {
	letters := "abcdefghijklmnopqrstuvwxyz"
	buf := make([]byte, n)
	for i := range buf {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		buf[i] = letters[idx.Int64()]
	}
	return string(buf)
}

// randInt returns a random int in [min, max).
func randInt(min, max int) int {
	n, _ := rand.Int(rand.Reader, big.NewInt(int64(max-min)))
	return int(n.Int64()) + min
}
