import json
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from pathlib import Path
import sys


def load_all_results(results_dir):
    results = []
    results_dir = Path(results_dir)

    # Load all benchmark files
    for result_file in results_dir.glob("benchmark_*_clients.json"):
        with open(result_file, "r") as f:
            results.extend(json.load(f))

    return pd.DataFrame(results)


def analyze_results(results_dir):
    # Load all results into a DataFrame
    df = load_all_results(results_dir)

    # Convert duration from nanoseconds to milliseconds
    df["duration_ms"] = df["duration"].apply(lambda x: float(x) / 1_000_000)

    # Calculate statistics per number of clients
    stats = []
    for num_clients in range(1, 11):
        client_df = df[df["num_clients"] == num_clients]

        # Calculate throughput
        total_time = (
            pd.to_datetime(client_df["timestamp"].max())
            - pd.to_datetime(client_df["timestamp"].min())
        ).total_seconds()
        successful_ops = client_df["success"].sum()
        throughput = successful_ops / total_time if total_time > 0 else 0

        # Calculate average latency
        avg_latency = client_df["duration_ms"].mean()

        stats.append(
            {
                "num_clients": num_clients,
                "throughput": throughput,
                "avg_latency": avg_latency,
                "success_rate": (successful_ops / len(client_df)) * 100,
            }
        )

    stats_df = pd.DataFrame(stats)

    # Create the plot
    fig, ax1 = plt.subplots(figsize=(12, 6))

    # Plot throughput
    color = "tab:blue"
    ax1.set_xlabel("Number of Clients")
    ax1.set_ylabel("Throughput (ops/second)", color=color)
    line1 = ax1.plot(
        stats_df["num_clients"],
        stats_df["throughput"],
        color=color,
        marker="o",
        label="Throughput",
    )
    ax1.tick_params(axis="y", labelcolor=color)

    # Create second y-axis for latency
    ax2 = ax1.twinx()
    color = "tab:red"
    ax2.set_ylabel("Average Latency (ms)", color=color)
    line2 = ax2.plot(
        stats_df["num_clients"],
        stats_df["avg_latency"],
        color=color,
        marker="s",
        label="Latency",
    )
    ax2.tick_params(axis="y", labelcolor=color)

    # Add legend
    lines = line1 + line2
    labels = [l.get_label() for l in lines]
    ax1.legend(lines, labels, loc="upper left")

    plt.title("GFS Append Performance vs Number of Clients")
    plt.grid(True, alpha=0.3)

    # Save the plot
    plt.savefig("performance_comparison.png")
    print("Performance comparison plot saved as 'performance_comparison.png'")

    # Print statistics
    print("\nDetailed Statistics:")
    print(stats_df.round(2).to_string(index=False))

    # Additional analysis
    print("\nKey Findings:")
    print(
        f"Best throughput: {stats_df['throughput'].max():.2f} ops/second "
        f"(with {stats_df.loc[stats_df['throughput'].idxmax(), 'num_clients']} clients)"
    )
    print(
        f"Lowest latency: {stats_df['avg_latency'].min():.2f} ms "
        f"(with {stats_df.loc[stats_df['avg_latency'].idxmin(), 'num_clients']} clients)"
    )
    print(f"Average success rate: {stats_df['success_rate'].mean():.2f}%")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python analyze_benchmark.py <results_directory>")
        sys.exit(1)

    analyze_results(sys.argv[1])
