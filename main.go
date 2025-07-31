package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/google/uuid"
)

type Record struct {
	ID     string  `json:"id"`
	Name   string  `json:"name"`
	Email  string  `json:"email"`
	Amount float64 `json:"amount"`
}

const (
	csvFile   = "dummy_data.csv"
	totalRows = 100_000
	batchSize = 1_000
)

func main() {
	// 1) Generate dummy CSV
	fmt.Println("📝 Generating CSV...")
	t0 := time.Now()
	if err := generateCSV(csvFile, totalRows); err != nil {
		log.Fatalf("CSV generation failed: %v", err)
	}
	fmt.Printf("🏁 CSV generated in %v\n\n", time.Since(t0))

	// 2) Load records into memory once
	records, err := loadRecords(csvFile)
	if err != nil {
		log.Fatalf("Failed to load CSV: %v", err)
	}

	// 3) Connect to Redis
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: "localhost:6379", DB: 0})
	defer client.Close()

	// 4) Run each approach
	runSetGet(ctx, client, records)
	fmt.Println()
	runMSetMGet(ctx, client, records, batchSize)
	fmt.Println()
	runHSetHGetAll(ctx, client, records, batchSize)
}

// ---------------- CSV Generation & Loading ----------------

func generateCSV(filename string, count int) error {
	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer f.Close()

	w := csv.NewWriter(f)
	w.Write([]string{"id", "name", "email", "amount"})
	for i := 0; i < count; i++ {
		id := uuid.New().String()
		name := fmt.Sprintf("%s %s", randStr(5), randStr(7))
		email := fmt.Sprintf("%s@%s.com", randStr(6), randStr(5))
		amt := math.Round(randFloat()*10000*100) / 100
		w.Write([]string{id, name, email, fmt.Sprintf("%.2f", amt)})
		if i%10000 == 0 {
			w.Flush()
			if err := w.Error(); err != nil {
				return err
			}
		}
	}
	w.Flush()
	return w.Error()
}

func randStr(n int) string {
	letters := "abcdefghijklmnopqrstuvwxyz"
	out := make([]byte, n)
	for i := range out {
		idx, _ := rand.Int(rand.Reader, big.NewInt(int64(len(letters))))
		out[i] = letters[idx.Int64()]
	}
	return string(out)
}

func randFloat() float64 {
	n, _ := rand.Int(rand.Reader, big.NewInt(1e9))
	return float64(n.Int64()) / 1e9
}

func loadRecords(filename string) ([]Record, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	r := csv.NewReader(bufio.NewReader(f))
	if _, err := r.Read(); err != nil {
		return nil, err
	}

	var recs []Record
	for {
		row, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		amt, _ := strconv.ParseFloat(row[3], 64)
		recs = append(recs, Record{
			ID:     row[0],
			Name:   row[1],
			Email:  row[2],
			Amount: amt,
		})
	}
	return recs, nil
}

// ---------------- Redis Helpers ----------------

func flushDB(ctx context.Context, client *redis.Client) {
	if err := client.FlushDB(ctx).Err(); err != nil {
		log.Fatalf("FlushDB failed: %v", err)
	}
}

func getMemStats(ctx context.Context, client *redis.Client) (usedBytes int64, usedHuman string) {
	info, err := client.Info(ctx, "memory").Result()
	if err != nil {
		log.Fatalf("Failed to INFO memory: %v", err)
	}
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, "used_memory:") {
			parts := strings.SplitN(line, ":", 2)
			usedBytes, _ = strconv.ParseInt(strings.TrimSpace(parts[1]), 10, 64)
		}
		if strings.HasPrefix(line, "used_memory_human:") {
			parts := strings.SplitN(line, ":", 2)
			usedHuman = strings.TrimSpace(parts[1])
		}
	}
	return
}

// ---------------- Approach 1: SET / GET ----------------

func runSetGet(ctx context.Context, client *redis.Client, recs []Record) {
	fmt.Println("=== Approach 1: SET & GET ===")
	flushDB(ctx, client)
	beforeBytes, beforeHuman := getMemStats(ctx, client)

	// Write (SET)
	t0 := time.Now()
	for _, r := range recs {
		data, _ := json.Marshal(r)
		if err := client.Set(ctx, r.ID, data, 0).Err(); err != nil {
			log.Fatalf("SET failed: %v", err)
		}
	}
	writeDur := time.Since(t0)
	afterBytes, afterHuman := getMemStats(ctx, client)
	deltaMB := float64(afterBytes-beforeBytes) / 1024 / 1024
	fmt.Printf("Write SET  : %v | Mem %s → %s (+%.2f MB)\n",
		writeDur, beforeHuman, afterHuman, deltaMB)

	// Read (GET)
	t1 := time.Now()
	for _, r := range recs {
		if _, err := client.Get(ctx, r.ID).Result(); err != nil {
			log.Fatalf("GET failed: %v", err)
		}
	}
	fmt.Printf("Read GET   : %v\n", time.Since(t1))
}

// ---------------- Approach 2: MSET / MGET ----------------

func runMSetMGet(ctx context.Context, client *redis.Client, recs []Record, batch int) {
	fmt.Println("=== Approach 2: MSET & MGET ===")
	flushDB(ctx, client)
	beforeBytes, beforeHuman := getMemStats(ctx, client)

	// Write (MSET in batches)
	t0 := time.Now()
	args := make([]interface{}, 0, batch*2)
	for i, r := range recs {
		data, _ := json.Marshal(r)
		args = append(args, r.ID, data)
		if (i+1)%batch == 0 {
			if err := client.MSet(ctx, args...).Err(); err != nil {
				log.Fatalf("MSET failed: %v", err)
			}
			args = args[:0]
		}
	}
	if len(args) > 0 {
		if err := client.MSet(ctx, args...).Err(); err != nil {
			log.Fatalf("MSET tail failed: %v", err)
		}
	}
	writeDur := time.Since(t0)
	afterBytes, afterHuman := getMemStats(ctx, client)
	deltaMB := float64(afterBytes-beforeBytes) / 1024 / 1024
	fmt.Printf("Write MSET : %v | Mem %s → %s (+%.2f MB)\n",
		writeDur, beforeHuman, afterHuman, deltaMB)

	// Read (MGET in batches)
	t1 := time.Now()
	keys := make([]string, 0, batch)
	for i, r := range recs {
		keys = append(keys, r.ID)
		if (i+1)%batch == 0 {
			if _, err := client.MGet(ctx, keys...).Result(); err != nil {
				log.Fatalf("MGET failed: %v", err)
			}
			keys = keys[:0]
		}
	}
	if len(keys) > 0 {
		if _, err := client.MGet(ctx, keys...).Result(); err != nil {
			log.Fatalf("MGET tail failed: %v", err)
		}
	}
	fmt.Printf("Read MGET  : %v\n", time.Since(t1))
}

// ---------------- Approach 3: HSET / HGETALL ----------------

func runHSetHGetAll(ctx context.Context, client *redis.Client, recs []Record, batch int) {
	fmt.Println("=== Approach 3: HSET & HGETALL ===")
	flushDB(ctx, client)
	beforeBytes, beforeHuman := getMemStats(ctx, client)

	// Write (HSET in pipeline batches)
	t0 := time.Now()
	pipe := client.Pipeline()
	cnt := 0
	for _, r := range recs {
		key := "rec:" + r.ID
		pipe.HSet(ctx, key,
			"id", r.ID,
			"name", r.Name,
			"email", r.Email,
			"amount", fmt.Sprintf("%.2f", r.Amount),
		)
		cnt++
		if cnt%batch == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Fatalf("HSET pipeline exec failed: %v", err)
			}
		}
	}
	if cnt%batch != 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("HSET final exec failed: %v", err)
		}
	}
	writeDur := time.Since(t0)
	afterBytes, afterHuman := getMemStats(ctx, client)
	deltaMB := float64(afterBytes-beforeBytes) / 1024 / 1024
	fmt.Printf("Write HSET : %v | Mem %s → %s (+%.2f MB)\n",
		writeDur, beforeHuman, afterHuman, deltaMB)

	// Read (HGETALL in pipeline batches)
	t1 := time.Now()
	pipe = client.Pipeline()
	cnt = 0
	for _, r := range recs {
		key := "rec:" + r.ID
		pipe.HGetAll(ctx, key)
		cnt++
		if cnt%batch == 0 {
			if _, err := pipe.Exec(ctx); err != nil {
				log.Fatalf("HGETALL pipeline exec failed: %v", err)
			}
		}
	}
	if cnt%batch != 0 {
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("HGETALL final exec failed: %v", err)
		}
	}
	fmt.Printf("Read HGETALL: %v\n", time.Since(t1))
}
