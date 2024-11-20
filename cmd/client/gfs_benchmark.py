import subprocess
import time
import random
import string
import concurrent.futures
import csv
from datetime import datetime
import statistics
import argparse
import os
import sys

class BenchmarkConfig:
    def __init__(self, num_clients, file_size, chunk_size, num_operations, 
                 read_percentage, gfs_cli_path):
        self.num_clients = num_clients
        self.file_size = file_size
        self.chunk_size = chunk_size
        self.num_operations = num_operations
        self.read_percentage = read_percentage
        self.gfs_cli_path = gfs_cli_path

class BenchmarkResult:
    def __init__(self, operation, duration, offset, size, success, client_id, timestamp):
        self.operation = operation
        self.duration = duration
        self.offset = offset  # Store offset instead of data
        self.size = size  # Store size instead of full data
        self.success = success
        self.client_id = client_id
        self.timestamp = timestamp

class GFSBenchmark:
    def __init__(self, config):
        self.config = config
        self.results = []

    def generate_random_data(self, size):
        return ''.join(random.choices(string.ascii_letters + string.digits, k=size))

    def run_gfs_command(self, command, client_id, offset=None, size=None):
        start_time = time.time()
        full_command = [
            self.config.gfs_cli_path
        ] + command
        
        try:
            result = subprocess.run(
                full_command, 
                capture_output=True, 
                text=True, 
                timeout=10  # Increased timeout
            )
            
            success = result.returncode == 0
            
            return BenchmarkResult(
                operation=' '.join(command),
                duration=time.time() - start_time,
                offset=offset,
                size=size,
                success=success,
                client_id=client_id,
                timestamp=start_time
            )
        except subprocess.TimeoutExpired:
            return BenchmarkResult(
                operation=' '.join(command),
                duration=time.time() - start_time,
                offset=offset,
                size=size,
                success=False,
                client_id=client_id,
                timestamp=start_time
            )
        except Exception as _:
            return BenchmarkResult(
                operation=' '.join(command),
                duration=time.time() - start_time,
                offset=offset,
                size=size,
                success=False,
                client_id=client_id,
                timestamp=start_time
            )

    def perform_write_operation(self, client_id, filename, offset, size):
        data = self.generate_random_data(size)
        command = ['write', filename, str(offset), data]
        return self.run_gfs_command(command, client_id, offset, size)

    def perform_read_operation(self, client_id, filename, offset, size):
        command = ['read', filename, str(offset), str(size)]
        return self.run_gfs_command(command, client_id, offset, size)

    def client_workload(self, client_id):
        filename = f"benchmark_file_{client_id}"
        
        # Create the file first
        create_result = self.run_gfs_command(['create', filename], client_id)
        local_results = [create_result]
        
        # If read percentage is > 0, generate 100 contiguous chunks first
        if self.config.read_percentage > 0:
            for i in range(100):
                offset = i * self.config.chunk_size
                write_result = self.perform_write_operation(
                    client_id, 
                    filename, 
                    offset, 
                    self.config.chunk_size
                )
                local_results.append(write_result)
        
        operations_completed = 0
        
        while operations_completed < self.config.num_operations:
            # Determine if this operation should be a read or write
            is_read = random.randint(1, 100) <= self.config.read_percentage
            
            if self.config.read_percentage == 100:
                # Random offsets within the first 100 chunks for reads
                max_offset = 100 * self.config.chunk_size - self.config.chunk_size
                offset = random.randint(0, max_offset)
            elif self.config.read_percentage == 0:
                # Sequential writes for write-only mode
                offset = (operations_completed * self.config.chunk_size) 
            else:
                # Random access for mixed workloads
                offset = random.randint(0, self.config.file_size - self.config.chunk_size)
            
            if is_read and self.config.read_percentage > 0:
                result = self.perform_read_operation(
                    client_id,
                    filename,
                    offset,
                    self.config.chunk_size
                )
            elif not is_read and self.config.read_percentage < 100:
                result = self.perform_write_operation(
                    client_id,
                    filename,
                    offset,
                    self.config.chunk_size
                )
            else:
                continue  # Skip this iteration if the operation doesn't match the configuration
            
            local_results.append(result)
            operations_completed += 1
            
            # Add a small delay between operations
            time.sleep(0.1)
        
        return local_results

    def run_benchmark(self):
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.config.num_clients) as executor:
            future_to_client = {
                executor.submit(self.client_workload, client_id): client_id 
                for client_id in range(self.config.num_clients)
            }
            
            for future in concurrent.futures.as_completed(future_to_client):
                client_id = future_to_client[future]
                try:
                    results = future.result()
                    self.results.extend(results)
                except Exception as _:
                    pass
        
        total_time = time.time() - start_time
        self.generate_report(total_time)

    def generate_report(self, total_time):
        # Calculate statistics
        read_results = [r for r in self.results if 'read' in r.operation and r.success]
        write_results = [r for r in self.results if 'write' in r.operation and r.success]
        
        read_durations = [r.duration for r in read_results]
        write_durations = [r.duration for r in write_results]
        
        read_throughput = sum(r.size for r in read_results) / total_time if read_results else 0
        write_throughput = sum(r.size for r in write_results) / total_time if write_results else 0
        
        # Generate timestamp for unique filenames
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        # Save detailed results to CSV
        csv_filename = f'benchmark_results_{timestamp}.csv'
        with open(csv_filename, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['Operation', 'Duration', 'Offset', 'Size', 'Success', 'Client', 'Timestamp'])
            for result in self.results:
                writer.writerow([
                    result.operation,
                    result.duration,
                    result.offset,
                    result.size,
                    result.success,
                    result.client_id,
                    result.timestamp
                ])

        # Print summary report
        print("\n=== GFS Benchmark Results ===")
        print(f"Total time: {total_time:.2f} seconds")
        print(f"Number of clients: {self.config.num_clients}")
        print(f"Operations per client: {self.config.num_operations}")
        print(f"Read Percentage: {self.config.read_percentage}%")
        print(f"\nSuccess Rates:")
        print(f"  Reads: {len(read_results)}/{len([r for r in self.results if 'read' in r.operation])}")
        print(f"  Writes: {len(write_results)}/{len([r for r in self.results if 'write' in r.operation])}")
        print("\nRead Statistics:")
        if read_durations:
            print(f"  Average latency: {statistics.mean(read_durations):.3f} seconds")
            print(f"  Throughput: {read_throughput / 1024 / 1024:.2f} MB/s")
        print("\nWrite Statistics:")
        if write_durations:
            print(f"  Average latency: {statistics.mean(write_durations):.3f} seconds")
            print(f"  Throughput: {write_throughput / 1024 / 1024:.2f} MB/s")
        print(f"\nDetailed results saved to: {csv_filename}")

def main():
    parser = argparse.ArgumentParser(description='GFS Benchmark Tool')
    parser.add_argument('--clients', type=int, default=1, help='Number of concurrent clients')
    parser.add_argument('--file-size', type=int, default=100*1024*1024, help='File size in bytes')
    parser.add_argument('--chunk-size', type=int, default=100*1024, help='Chunk size in bytes')
    parser.add_argument('--operations', type=int, default=10, help='Number of operations per client')
    parser.add_argument('--read-ratio', type=int, default=50, help='Percentage of read operations (0-100)')
    parser.add_argument('--gfs-cli-path', type=str, default='./gfs-cli', help='Path to the GFS CLI executable')

    args = parser.parse_args()

    # Input validation for read ratio
    if args.read_ratio < 0 or args.read_ratio > 100:
        sys.exit(1)

    # Ensure absolute paths
    gfs_cli_path = os.path.abspath(args.gfs_cli_path)

    # Build the GFS CLI if needed
    if not os.path.exists(gfs_cli_path):
        try:
            build_process = subprocess.run(
                ['go', 'build', '-o', gfs_cli_path, 'gfs_benchmark_main.go'],
                capture_output=True,
                text=True,
                cwd=os.path.dirname(gfs_cli_path)
            )
            if build_process.returncode != 0:
                sys.exit(1)
        except Exception as _:
            sys.exit(1)

    config = BenchmarkConfig(
        num_clients=args.clients,
        file_size=args.file_size,
        chunk_size=args.chunk_size,
        num_operations=args.operations,
        read_percentage=args.read_ratio,
        gfs_cli_path=gfs_cli_path,
    )

    benchmark = GFSBenchmark(config)
    benchmark.run_benchmark()

if __name__ == '__main__':
    main()