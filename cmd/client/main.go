package main

import (
	"bufio"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"context"

	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/client"
	"github.com/spf13/cobra"
	"github.com/fatih/color"
)

var (
	gfsClient *client.Client
	config    *client.ClientConfig
)

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var rootCmd = &cobra.Command{
		Use:   "gfs-cli",
		Short: "GFS Client CLI",
		Long:  `A command-line interface for interacting with the Google File System (GFS) implementation.`,
		Run: func(cmd *cobra.Command, args []string) {
			// Initialize client
			setupClient(cmd)
			// Start the interactive CLI
			startCLI()
		},
	}

	rootCmd.PersistentFlags().String("config", "../../configs/client-config.yml", "path to client configuration file")

	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func setupClient(cmd *cobra.Command) {
	configPath, err := cmd.Flags().GetString("config")
	if err != nil {
		log.Fatalf("Failed to get config flag: %v", err)
	}
	if configPath == "" {
		log.Fatal("Configuration path cannot be empty")
	}

	gfsClient, err = client.NewClient(configPath)
	if err != nil {
		log.Fatalf("Failed to create GFS client: %v", err)
	}
}

func loadConfig(path string) (*client.ClientConfig, error) {
	if path == "" {
		return nil, fmt.Errorf("configuration path cannot be empty")
	}

	absPath, err := filepath.Abs(path)
	if err != nil {
		return nil, fmt.Errorf("invalid config path: %v", err)
	}

	if _, err := os.Stat(absPath); os.IsNotExist(err) {
		return nil, fmt.Errorf("config file does not exist: %s", absPath)
	}

	config, err := client.LoadConfig(absPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load config from %s: %v", absPath, err)
	}

	clientConfig := config.ToClientConfig()
	return clientConfig, nil
}

func startCLI() {
	reader := bufio.NewReader(os.Stdin)
	color.Green("Welcome to GFS Client!")
	color.Yellow("Type 'help' for available commands or 'exit' to quit")

	for {
		fmt.Print("gfs> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			color.Red("Error reading input: %v", err)
			continue
		}

		input = strings.TrimSpace(input)
		if input == "" {
			continue
		}

		if !processCommand(input) {
			break // Exit the loop if processCommand returns false
		}
	}
}

func processCommand(input string) bool {
	args := strings.Fields(input)
	if len(args) == 0 {
		return true
	}

	cmd := args[0]
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	switch cmd {
	case "exit", "quit":
		color.Yellow("Goodbye!")
		return false

	case "help":
		printHelp()

	case "create":
		if len(args) < 2 {
			color.Red("Usage: create <filename>")
			return true
		}
		handleCreate(ctx, args[1])

	case "rename":
		if len(args) < 3 {
			color.Red("Usage: rename <old_filename> <new_filename>")
			return true
		}
		handleRename(ctx, args[1], args[2])

	case "delete":
		if len(args) < 2 {
			color.Red("Usage: delete <filename>")
			return true
		}
		handleDelete(ctx, args[1])

	case "read":
		if len(args) < 4 {
            color.Red("Usage: read <filename> <offset> <length>")
            return true
        }
        offset, err := strconv.ParseInt(args[2], 10, 64)
        if err != nil {
            color.Red("Invalid offset: %v", err)
            return true
        }
        length, err := strconv.ParseInt(args[3], 10, 64)
        if err != nil {
            color.Red("Invalid length: %v", err)
            return true
        }
        handleRead(ctx, args[1], offset, length)


	case "write":
		if len(args) < 3 {
			color.Red("Usage: write <filename> <offset> <data>")
			return true
		}
		
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			color.Red("Invalid offset: %v", err)
			return true
		}
		
		content := strings.Join(args[3:], " ")
		handleWrite(ctx, args[1], offset, content)

	case "writefile":
		if len(args) < 3 {
			color.Red("Usage: writefile <gfs_filename> <offset> <local_filepath>")
			return true
		}
		offset, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			color.Red("Invalid offset: %v", err)
			return true
		}
		handleWriteFile(ctx, args[1], offset, args[3])

	case "chunks":
		if len(args) < 4 {
			color.Red("Usage: chunks <filename> <start_chunk> <end_chunk>")
			return true
		}
		startChunk, _ := strconv.ParseInt(args[2], 10, 64)
		endChunk, _ := strconv.ParseInt(args[3], 10, 64)
		handleGetChunks(ctx, args[1], startChunk, endChunk)

	case "push":
        if len(args) < 3 {
            color.Red("Usage: push <chunk_handle> <data>")
            return true
        }
        
		data := strings.Join(args[2:], " ")
        handlePushData(ctx, args[1], data)

	default:
		color.Red("Unknown command: %s", cmd)
	}

	return true
}

func printHelp() {
	color.Cyan("Available Commands:")
	fmt.Println("  create <filename>                           - Create a new file")
	fmt.Println("  delete <filename>                           - Delete a file")
	fmt.Println("  rename <old_filename> <new_filename>        - Rename a file")
	fmt.Println("  read <filename> <offset> <length> - Read file contents")
	fmt.Println("  write <filename> <offset> <data> - Write content to a file")
	fmt.Println("  writefile <gfs_filename> <offset> <local_filepath> - Write file contents from local file")
	fmt.Println("  chunks <filename> <start_chunk> <end_chunk> - Get chunk information")
	fmt.Println("  push <chunk_handle> <data>     - Push data to a chunk")
	fmt.Println("  ls                                          - List all files")
	fmt.Println("  help                                        - Show this help")
	fmt.Println("  exit                                        - Exit the shell")
}

func handleCreate(ctx context.Context, filename string) {
	err := gfsClient.Create(ctx, filename)
	if err != nil {
		color.Red("Failed to create file: %v", err)
		return
	}
	color.Green("File created successfully: %s", filename)
}

func handleRename(ctx context.Context, old_filename string, new_filename string) {
	err := gfsClient.Rename(ctx, old_filename, new_filename)
	if err != nil {
		color.Red("Failed to rename file: %v", err)
		return
	}
	color.Green("File %s renamed successfully to: %s", old_filename, new_filename)
}

func handleDelete(ctx context.Context, filename string) {
	err := gfsClient.Delete(ctx, filename)
	if err != nil {
		color.Red("Failed to delete file: %v", err)
		return
	}
	color.Green("File deleted successfully: %s", filename)
}

func handleRead(ctx context.Context, filename string, offset, length int64) {
    data, err := gfsClient.Read(ctx, filename, offset, length)
    if err != nil {
        color.Red("Failed to read file: %v", err)
        return
    }

    color.Green("Successfully read %d bytes:", len(data))
    fmt.Println(string(data))
}


func handleWrite(ctx context.Context, filename string, offset int64, content string) {
    data := []byte(content)
    written, err := gfsClient.Write(ctx, filename, offset, data)
    if err != nil {
        color.Red("Failed to write to file: %v", err)
        return
    }
    color.Green("Successfully wrote %d bytes to file", written)
}

func handleWriteFile(ctx context.Context, gfsFilename string, offset int64, localFilepath string) {
	// Check if local file exists
	if _, err := os.Stat(localFilepath); os.IsNotExist(err) {
		color.Red("Local file does not exist: %s", localFilepath)
		return
	}

	// Read the local file
	data, err := ioutil.ReadFile(localFilepath)
	if err != nil {
		color.Red("Failed to read local file: %v", err)
		return
	}

	// Write the data to GFS
	written, err := gfsClient.Write(ctx, gfsFilename, offset, data)
	if err != nil {
		color.Red("Failed to write to GFS file: %v", err)
		return
	}
	color.Green("Successfully wrote %d bytes from %s to GFS file %s", written, localFilepath, gfsFilename)
}

func handleGetChunks(ctx context.Context, filename string, startChunk, endChunk int64) {
	chunks, err := gfsClient.GetChunkInfo(ctx, filename, startChunk, endChunk)
	if err != nil {
		color.Red("Failed to get chunk info: %v", err)
		return
	}

	color.Green("Chunk Information for %s (chunks %d to %d):", filename, startChunk, endChunk)
	for idx, chunk := range chunks {
		color.Cyan("Chunk %d:", idx)
		fmt.Printf("  Handle: %s\n", chunk.ChunkHandle.Handle)
		fmt.Printf("  Primary: %s\n", chunk.PrimaryLocation.ServerId)
		fmt.Printf("  Secondaries: %v\n", chunk.SecondaryLocations)
	}
}

func handlePushData(ctx context.Context, chunkHandle string, data string) {
    operationId, err := gfsClient.PushDataToPrimary(ctx, chunkHandle, []byte(data))
    if err != nil {
        color.Red("Failed to push data: %v", err)
        return
    }
	color.Green("OperationId: %s", operationId)
    color.Green("Data pushed successfully to chunk %s", chunkHandle)
}
