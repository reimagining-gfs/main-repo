// package main

// import (
// 	"context"
// 	"fmt"
// 	"io/ioutil"
// 	"log"
// 	"os"
// 	"strconv"
// 	"time"

// 	"github.com/Mit-Vin/GFS-Distributed-Systems/internal/client"
// 	"github.com/spf13/cobra"
// 	"github.com/fatih/color"
// )
// c
// var gfsClient *client.Client

// func main() {
// 	log.SetFlags(log.LstdFlags | log.Lshortfile)

// 	var rootCmd = &cobra.Command{
// 		Use:   "gfs-cli",
// 		Short: "GFS Client CLI",
// 		Long:  `A command-line interface for interacting with the Google File System (GFS) implementation.`,
// 	}

// 	// Configuration flag
// 	rootCmd.PersistentFlags().String("config", "../../configs/client-config.yml", "path to client configuration file")

// 	// Create command
// 	var createCmd = &cobra.Command{
// 		Use:   "create <filename>",
// 		Short: "Create a new file",
// 		Args:  cobra.ExactArgs(1),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			filename := args[0]
// 			err := gfsClient.Create(ctx, filename)
// 			if err != nil {
// 				color.Red("Failed to create file: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("File created successfully: %s", filename)
// 		},
// 	}
// 	rootCmd.AddCommand(createCmd)

// 	// Delete command
// 	var deleteCmd = &cobra.Command{
// 		Use:   "delete <filename>",
// 		Short: "Delete a file",
// 		Args:  cobra.ExactArgs(1),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			filename := args[0]
// 			err := gfsClient.Delete(ctx, filename)
// 			if err != nil {
// 				color.Red("Failed to delete file: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("File deleted successfully: %s", filename)
// 		},
// 	}
// 	rootCmd.AddCommand(deleteCmd)

// 	// Rename command
// 	var renameCmd = &cobra.Command{
// 		Use:   "rename <old_filename> <new_filename>",
// 		Short: "Rename a file",
// 		Args:  cobra.ExactArgs(2),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			oldFilename, newFilename := args[0], args[1]
// 			err := gfsClient.Rename(ctx, oldFilename, newFilename)
// 			if err != nil {
// 				color.Red("Failed to rename file: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("File %s renamed successfully to: %s", oldFilename, newFilename)
// 		},
// 	}
// 	rootCmd.AddCommand(renameCmd)

// 	// Read command
// 	var readCmd = &cobra.Command{
// 		Use:   "read <filename> <offset> <length>",
// 		Short: "Read file contents",
// 		Args:  cobra.ExactArgs(3),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			filename := args[0]
// 			offset, err := strconv.ParseInt(args[1], 10, 64)
// 			if err != nil {
// 				color.Red("Invalid offset: %v", err)
// 				os.Exit(1)
// 			}
// 			length, err := strconv.ParseInt(args[2], 10, 64)
// 			if err != nil {
// 				color.Red("Invalid length: %v", err)
// 				os.Exit(1)
// 			}

// 			data, err := gfsClient.Read(ctx, filename, offset, length)
// 			if err != nil {
// 				color.Red("Failed to read file: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("Successfully read %d bytes:", len(data))
// 			fmt.Println(string(data))
// 		},
// 	}
// 	rootCmd.AddCommand(readCmd)

// 	// Write command
// 	var writeCmd = &cobra.Command{
// 		Use:   "write <filename> <offset> <data>",
// 		Short: "Write content to a file",
// 		Args:  cobra.MinimumNArgs(3),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			filename := args[0]
// 			offset, err := strconv.ParseInt(args[1], 10, 64)
// 			if err != nil {
// 				color.Red("Invalid offset: %v", err)
// 				os.Exit(1)
// 			}
// 			content := args[2]

// 			data := []byte(content)
// 			written, err := gfsClient.Write(ctx, filename, offset, data)
// 			if err != nil {
// 				color.Red("Failed to write to file: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("Successfully wrote %d bytes to file", written)
// 		},
// 	}
// 	rootCmd.AddCommand(writeCmd)

// 	// Write file command
// 	var writeFileCmd = &cobra.Command{
// 		Use:   "writefile <gfs_filename> <offset> <local_filepath>",
// 		Short: "Write file contents from local file",
// 		Args:  cobra.ExactArgs(3),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			gfsFilename := args[0]
// 			offset, err := strconv.ParseInt(args[1], 10, 64)
// 			if err != nil {
// 				color.Red("Invalid offset: %v", err)
// 				os.Exit(1)
// 			}
// 			localFilepath := args[2]

// 			// Check if local file exists
// 			if _, err := os.Stat(localFilepath); os.IsNotExist(err) {
// 				color.Red("Local file does not exist: %s", localFilepath)
// 				os.Exit(1)
// 			}

// 			// Read the local file
// 			data, err := ioutil.ReadFile(localFilepath)
// 			if err != nil {
// 				color.Red("Failed to read local file: %v", err)
// 				os.Exit(1)
// 			}

// 			// Write the data to GFS
// 			written, err := gfsClient.Write(ctx, gfsFilename, offset, data)
// 			if err != nil {
// 				color.Red("Failed to write to GFS file: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("Successfully wrote %d bytes from %s to GFS file %s", written, localFilepath, gfsFilename)
// 		},
// 	}
// 	rootCmd.AddCommand(writeFileCmd)

// 	// Get chunks command
// 	var getChunksCmd = &cobra.Command{
// 		Use:   "chunks <filename> <start_chunk> <end_chunk>",
// 		Short: "Get chunk information",
// 		Args:  cobra.ExactArgs(3),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			filename := args[0]
// 			startChunk, err := strconv.ParseInt(args[1], 10, 64)
// 			if err != nil {
// 				color.Red("Invalid start chunk: %v", err)
// 				os.Exit(1)
// 			}
// 			endChunk, err := strconv.ParseInt(args[2], 10, 64)
// 			if err != nil {
// 				color.Red("Invalid end chunk: %v", err)
// 				os.Exit(1)
// 			}

// 			chunks, err := gfsClient.GetChunkInfo(ctx, filename, startChunk, endChunk)
// 			if err != nil {
// 				color.Red("Failed to get chunk info: %v", err)
// 				os.Exit(1)
// 			}

// 			color.Green("Chunk Information for %s (chunks %d to %d):", filename, startChunk, endChunk)
// 			for idx, chunk := range chunks {
// 				color.Cyan("Chunk %d:", idx)
// 				fmt.Printf("  Handle: %s\n", chunk.ChunkHandle.Handle)
// 				fmt.Printf("  Primary: %s\n", chunk.PrimaryLocation.ServerId)
// 				fmt.Printf("  Secondaries: %v\n", chunk.SecondaryLocations)
// 			}
// 		},
// 	}
// 	rootCmd.AddCommand(getChunksCmd)

// 	// Push data command
// 	var pushDataCmd = &cobra.Command{
// 		Use:   "push <chunk_handle> <data>",
// 		Short: "Push data to a chunk",
// 		Args:  cobra.MinimumNArgs(2),
// 		Run: func(cmd *cobra.Command, args []string) {
// 			setupClient(cmd)
// 			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
// 			defer cancel()

// 			chunkHandle := args[0]
// 			data := args[1]

// 			operationId, err := gfsClient.PushDataToPrimary(ctx, chunkHandle, []byte(data))
// 			if err != nil {
// 				color.Red("Failed to push data: %v", err)
// 				os.Exit(1)
// 			}
// 			color.Green("OperationId: %s", operationId)
// 			color.Green("Data pushed successfully to chunk %s", chunkHandle)
// 		},
// 	}
// 	rootCmd.AddCommand(pushDataCmd)

// 	if err := rootCmd.Execute(); err != nil {
// 		fmt.Println(err)
// 		os.Exit(1)
// 	}
// }

// func setupClient(cmd *cobra.Command) {
// 	configPath, err := cmd.Flags().GetString("config")
// 	if err != nil {
// 		log.Fatalf("Failed to get config flag: %v", err)
// 	}
// 	if configPath == "" {
// 		log.Fatal("Configuration path cannot be empty")
// 	}

// 	gfsClient, err = client.NewClient(configPath)
// 	if err != nil {
// 		log.Fatalf("Failed to create GFS client: %v", err)
// 	}
// }