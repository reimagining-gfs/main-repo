package main

// import (
// 	"context"
// 	"encoding/json"
// 	"flag"
// 	"fmt"
// 	"log"
// 	"os"
// 	"path/filepath"
// 	"sync"
// 	"time"

// 	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/client"
// )

// type BenchmarkResult struct {
// 	ClientID     int           `json:"client_id"`
// 	NumClients   int           `json:"num_clients"`
// 	OperationID  int           `json:"operation_id"`
// 	DataSize     int           `json:"data_size"`
// 	Duration     time.Duration `json:"duration"`
// 	Success      bool          `json:"success"`
// 	ErrorMessage string        `json:"error_message,omitempty"`
// 	Timestamp    time.Time     `json:"timestamp"`
// 	AppendOffset int64         `json:"append_offset"`
// }

// func runBenchmark(numClients int, opsPerClient int, dataSize int, configPath string) []BenchmarkResult {
// 	// Create a single shared file for all clients
// 	filename := fmt.Sprintf("benchmark_shared_file_%d.txt", numClients)
// 	setupClient, err := client.NewClient(configPath)
// 	if err != nil {
// 		log.Fatalf("Failed to create setup client: %v", err)
// 	}

// 	// Create the shared file
// 	ctx := context.Background()
// 	err = setupClient.Create(ctx, filename)
// 	if err != nil {
// 		log.Fatalf("Failed to create shared file: %v", err)
// 	}

// 	var results []BenchmarkResult
// 	var resultsMutex sync.Mutex
// 	var wg sync.WaitGroup
// 	wg.Add(numClients)

// 	// Generate test data
// 	testData := make([]byte, dataSize)
// 	for i := range testData {
// 		testData[i] = 'A' + byte(i%26)
// 	}

// 	// Start clients
// 	for i := 0; i < numClients; i++ {
// 		go func(clientID int) {
// 			defer wg.Done()

// 			gfsClient, err := client.NewClient(configPath)
// 			if err != nil {
// 				log.Printf("Client %d failed to initialize: %v", clientID, err)
// 				return
// 			}

// 			for op := 0; op < opsPerClient; op++ {
// 				start := time.Now()
// 				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

// 				result := BenchmarkResult{
// 					ClientID:    clientID,
// 					NumClients:  numClients,
// 					OperationID: op,
// 					DataSize:    dataSize,
// 					Timestamp:   start,
// 					Success:     true,
// 				}

// 				offset, err := gfsClient.Append(ctx, filename, testData)
// 				result.Duration = time.Since(start)
// 				result.AppendOffset = offset

// 				if err != nil {
// 					result.Success = false
// 					result.ErrorMessage = err.Error()
// 				}

// 				resultsMutex.Lock()
// 				results = append(results, result)
// 				resultsMutex.Unlock()

// 				cancel()
// 			}
// 		}(i)
// 	}

// 	wg.Wait()
// 	return results
// }

// func main() {
// 	opsPerClient := flag.Int("ops", 100, "number of operations per client")
// 	dataSize := flag.Int("size", 1024, "size of data to append in bytes")
// 	configPath := flag.String("config", "../../configs/client-config.yml", "path to client configuration file")
// 	outputDir := flag.String("outputdir", "benchmark_results", "output directory for results")
// 	flag.Parse()

// 	// Create output directory if it doesn't exist
// 	err := os.MkdirAll(*outputDir, 0755)
// 	if err != nil {
// 		log.Fatalf("Failed to create output directory: %v", err)
// 	}

// 	// Run benchmarks for 1 to 10 clients
// 	for numClients := 1; numClients <= 10; numClients++ {
// 		fmt.Printf("Running benchmark with %d clients...\n", numClients)
// 		results := runBenchmark(numClients, *opsPerClient, *dataSize, *configPath)

// 		// Save results to file
// 		outputFile := filepath.Join(*outputDir, fmt.Sprintf("benchmark_%d_clients.json", numClients))
// 		resultFile, err := os.Create(outputFile)
// 		if err != nil {
// 			log.Fatalf("Failed to create output file: %v", err)
// 		}

// 		encoder := json.NewEncoder(resultFile)
// 		encoder.SetIndent("", "  ")
// 		if err := encoder.Encode(results); err != nil {
// 			resultFile.Close()
// 			log.Fatalf("Failed to write results: %v", err)
// 		}
// 		resultFile.Close()
// 	}

// 	fmt.Printf("All benchmarks complete. Results written to %s\n", *outputDir)
// }

func benchmark_append() {
	// uncomment above code
}
