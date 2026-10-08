import duckdb
import pickle
import time

def value_counts_files():
    start = time.time()
    
    result = duckdb.sql("""
        SELECT 
            regexp_extract(path, '([^/]+)$') AS file_name,
            branch,
            COUNT(*) AS count
        FROM 
            read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true) 
        GROUP BY 
            regexp_extract(path, '([^/]+)$'),
            branch
        ORDER BY 
            count DESC;                        
    """).fetchdf()
    
    print("Execution time: ", time.time() - start)

    with open('../results/value_counts_files_with_branch.pkl', 'wb') as f:
        pickle.dump(result, f)
        
if __name__ == "__main__":
    value_counts_files()