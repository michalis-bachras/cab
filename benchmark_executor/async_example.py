import asyncio
import time
import duckdb

async def fetch_data(source, delay):
    """Simulates fetching data from different sources"""
    print(f"Starting to fetch from {source}...")
    await asyncio.sleep(delay)  # Simulates network/disk I/O
    print(f"Finished fetching from {source}")
    return f"Data from {source}"

async def sequential_approach():
    """The old way - one after another"""
    start_time = time.time()

    data1 = await fetch_data("Database", 3)
    data2 = await fetch_data("API", 2)
    data3 = await fetch_data("File", 1)

    end_time = time.time()
    print(f"Sequential took {end_time - start_time:.2f} seconds")
    return [data1, data2, data3]

async def concurrent_approach():
    """The async way - all at once!"""
    start_time = time.time()

    # Create all tasks at once
    tasks = [
        fetch_data("Database", 3),
        fetch_data("API", 2),
        fetch_data("File", 1)
    ]

    # Wait for all to complete
    results = await asyncio.gather(*tasks)

    end_time = time.time()
    print(f"Concurrent took {end_time - start_time:.2f} seconds")
    return results

# Compare both approaches
async def compare_approaches():
    print("=== Sequential Approach ===")
    await sequential_approach()

    print("\n=== Concurrent Approach ===")
    await concurrent_approach()

if __name__ == "__main__":
    conn = duckdb.connect()
    asyncio.run(compare_approaches())