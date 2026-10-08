import pickle
import duckdb
import time

# .ssh/*, id_dsa*, id_rsa*, secret,

def secret_files():
    start = time.time()

    result = duckdb.sql("""
        SELECT * 
        FROM read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true) 
        WHERE (
            path LIKE '%/.ssh%' OR 
            path LIKE '%/id_dsa' OR
            path LIKE '%/id_ecdsa' OR
            path LIKE '%/id_rsa' OR
            path LIKE '%secret%' OR
            path LIKE '%/.env%' OR
            path LIKE '%credential%' OR
            path LIKE '%/secring.gpg' OR
            path LIKE '%/.npmrc' OR 
            path LIKE '%/.pypirc' OR
            path LIKE '%/.netrc' OR
            path LIKE '%key%' OR
            path LIKE '%.pem' OR
            path LIKE '%.pfx' OR
            path LIKE '%.p12' OR
            path LIKE '%.jks'
        )
    """).fetchdf()

    print("Execution time: ", time.time() - start)

    with open('../results/secret_files_small.pkl', 'wb') as f:
        pickle.dump(result, f)
    

if __name__ == "__main__":
    secret_files()