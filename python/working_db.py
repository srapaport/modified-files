import duckdb
import pickle

result = duckdb.sql("""
    SELECT * 
    FROM read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true) 
    WHERE branch = 'refs/heads/main'
    AND (
        lower(path) LIKE '%license%' OR 
        lower(path) LIKE '%licence%'
    )
""").fetchdf()

with open('../results/result.pkl', 'wb') as f:
    pickle.dump(result, f)
    
result_grade = duckdb.sql("""
    SELECT * 
    FROM read_csv_auto('../results/grades.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true) 
    WHERE amount_snap > 1
    AND amount_rev > 1
""").fetchdf()

with open("../results/grades.pkl", 'wb') as f:
    pickle.dump(result_grade, f)
    
#25min