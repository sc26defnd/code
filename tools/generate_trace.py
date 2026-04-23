import numpy as np

def generate_zipfian_trace(num_keys, total_requests, filename):
    
    # theta = 0.99
    a = 1.01 + 0.99 
    
    raw_zipf = np.random.zipf(a, total_requests * 3) 
    
    filtered_trace = raw_zipf[raw_zipf <= num_keys] - 1
    
    final_trace = filtered_trace[:total_requests]
    
    with open(filename, 'w') as f:
        for req in final_trace:
            f.write(f"{req}\n")
            
    print(f"success!")
    
    unique, counts = np.unique(final_trace, return_counts=True)
    sorted_counts = np.sort(counts)[::-1]
    top_20_percent = int(num_keys * 0.2)
    hot_ratio = np.sum(sorted_counts[:top_20_percent]) / total_requests * 100
    print(f"20% hot file, {hot_ratio:.2f}% read traffic!")

# 100 stripes
generate_zipfian_trace(num_keys=100, total_requests=1000000, filename="./../data/trace.txt")