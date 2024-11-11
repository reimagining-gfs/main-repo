package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"path/filepath"
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

	case "delete":
		if len(args) < 2 {
			color.Red("Usage: delete <filename>")
			return true
		}
		handleDelete(ctx, args[1])

	case "read":
		if len(args) < 2 {
			color.Red("Usage: read <filename>")
			return true
		}
		handleRead(ctx, args[1])

	case "write":
		if len(args) < 3 {
			color.Red("Usage: write <filename> <content>")
			return true
		}
		content := strings.Join(args[2:], " ")
		handleWrite(ctx, args[1], content)

	case "ls":
		handleList(ctx)

	default:
		color.Red("Unknown command: %s", cmd)
	}

	return true
}

func printHelp() {
	color.Cyan("Available Commands:")
	fmt.Println("  create <filename>              - Create a new file")
	fmt.Println("  delete <filename>              - Delete a file")
	fmt.Println("  read <filename>                - Read file contents")
	fmt.Println("  write <filename> <content>     - Write content to a file")
	fmt.Println("  ls                             - List all files")
	fmt.Println("  help                           - Show this help")
	fmt.Println("  exit                           - Exit the shell")
}

func handleCreate(ctx context.Context, filename string) {
	err := gfsClient.Create(ctx, filename)
	if err != nil {
		color.Red("Failed to create file: %v", err)
		return
	}
	color.Green("File created successfully: %s", filename)
}

func handleDelete(ctx context.Context, filename string) {
	err := gfsClient.Delete(ctx, filename)
	if err != nil {
		color.Red("Failed to delete file: %v", err)
		return
	}
	color.Green("File deleted successfully: %s", filename)
}

func handleRead(ctx context.Context, filename string) {
	// TODO: Implement read functionality when available in the chunk-server
	color.Yellow("Read functionality not implemented yet")
}

func handleWrite(ctx context.Context, filename, content string) {
	// TODO: Implement write functionality when available in the chunk-server
	color.Yellow("Write functionality not implemented yet")
}

func handleList(ctx context.Context) {
	// TODO: Implement listing functionality when available in the master
	color.Yellow("Listing functionality not implemented yet")
}
