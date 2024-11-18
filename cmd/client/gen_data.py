import os
import argparse
import random
import string

def generate_random_data(size_mb, output_file):
    """
    Generate a file with random data of specified size in MB.

    :param size_mb: Size of the file in megabytes.
    :param output_file: Name of the output file.
    """
    size_bytes = size_mb * 1024 * 1024  # Convert size from MB to bytes
    chunk_size = 1024 * 1024  # Write in 1 MB chunks for efficiency
    chars = string.ascii_letters + string.digits + string.punctuation  # Character pool
    
    with open(output_file, 'w') as f:
        for _ in range(size_mb):
            data = ''.join(random.choices(chars, k=chunk_size))  # Generate random chunk
            f.write(data[:chunk_size])  # Ensure only 1 MB is written at a time

    print(f"File '{output_file}' of size {size_mb} MB has been created.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a file with random data of specified size.")
    parser.add_argument("size", type=int, help="Size of the file in MB.")
    parser.add_argument("output", type=str, help="Name of the output file.")

    args = parser.parse_args()

    try:
        generate_random_data(args.size, args.output)
    except Exception as e:
        print(f"Error generating file: {e}")
