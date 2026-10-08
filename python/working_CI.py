import duckdb
import pickle
import time

# result = duckdb.sql("""
#     SELECT * 
#     FROM read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true) 
#     WHERE 
#         lower(path) LIKE '%.github/workflows/%.yml' OR
#         lower(path) LIKE '%.github/workflows/%.yaml' OR
#         lower(path) LIKE '%.github/actions/%' OR
#         lower(path) LIKE '%/.travis.yml' OR
#         lower(path) LIKE '%/.circleci/config.yml' OR
#         lower(path) LIKE '%/.circleci/config.yaml' OR
#         lower(path) LIKE '%/.gitlab-ci.yml' OR
#         lower(path) LIKE '%/jenkinsfile' OR
#         lower(path) LIKE '%/jenkins.yml' OR
#         lower(path) LIKE '%/azure-pipelines.yml' OR
#         lower(path) LIKE '%/appveyor.yml' OR
#         lower(path) LIKE '%/.drone.yml' OR
#         lower(path) LIKE '%/bitbucket-pipelines.yml' OR
#         lower(path) LIKE '%/ci/%' OR
#         lower(path) LIKE '%/continuous-integration/%' OR
#         lower(path) LIKE '%/build.yml' OR
#         lower(path) LIKE '%/build.yaml'
# """).fetchdf()

# with open('../results/results_CI.pkl', 'wb') as f:
#     pickle.dump(result, f)


# start = time.time()

# ci_only_repos = duckdb.sql("""
#     WITH all_files AS (
#         SELECT *, COUNT(*) as total_files
#         FROM read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true)
#         GROUP BY origin
#     ),
#     ci_files AS (
#         SELECT *, COUNT(*) as ci_files
#         FROM read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true)
#         WHERE 
#             lower(path) LIKE '%.github/workflows/%.yml' OR
#             lower(path) LIKE '%.github/workflows/%.yaml' OR
#             lower(path) LIKE '%.github/actions/%' OR
#             lower(path) LIKE '%/.travis.yml' OR
#             lower(path) LIKE '%/.circleci/config.yml' OR
#             lower(path) LIKE '%/.circleci/config.yaml' OR
#             lower(path) LIKE '%/.gitlab-ci.yml' OR
#             lower(path) LIKE '%/jenkinsfile' OR
#             lower(path) LIKE '%/jenkins.yml' OR
#             lower(path) LIKE '%/azure-pipelines.yml' OR
#             lower(path) LIKE '%/appveyor.yml' OR
#             lower(path) LIKE '%/.drone.yml' OR
#             lower(path) LIKE '%/bitbucket-pipelines.yml' OR
#             lower(path) LIKE '%/ci/%' OR
#             lower(path) LIKE '%/continuous-integration/%' OR
#             lower(path) LIKE '%/build.yml' OR
#             lower(path) LIKE '%/build.yaml'
#         GROUP BY origin
#     )
#     SELECT *
#     FROM all_files af
#     JOIN ci_files cf ON af.origin = cf.origin
#     WHERE af.total_files = cf.ci_files
# """).fetchdf()

# end = time.time()

# print("time taken: ", end-start) # 34min

# with open('../results/results_CI_v2.pkl', 'wb') as f:
#     pickle.dump(ci_only_repos, f)
    
start = time.time()
 
ci_only_repos = duckdb.sql("""
    WITH ci_flags AS (
        SELECT *,
            CASE WHEN 
                lower(path) LIKE '%.github/workflows/%.yml' OR
                lower(path) LIKE '%.github/workflows/%.yaml' OR
                lower(path) LIKE '%.github/actions/%' OR
                lower(path) LIKE '%/.travis.yml' OR
                lower(path) LIKE '%/.circleci/config.yml' OR
                lower(path) LIKE '%/.circleci/config.yaml' OR
                lower(path) LIKE '%/.gitlab-ci.yml' OR
                lower(path) LIKE '%/jenkinsfile' OR
                lower(path) LIKE '%/jenkins.yml' OR
                lower(path) LIKE '%/azure-pipelines.yml' OR
                lower(path) LIKE '%/appveyor.yml' OR
                lower(path) LIKE '%/.drone.yml' OR
                lower(path) LIKE '%/bitbucket-pipelines.yml' OR
                lower(path) LIKE '%/ci/%' OR
                lower(path) LIKE '%/continuous-integration/%' OR
                lower(path) LIKE '%/build.yml' OR
                lower(path) LIKE '%/build.yaml'
                THEN 1 ELSE 0 
            END AS is_ci_file
        FROM read_csv_auto('../results/modified_files.csv', strict_mode=false, max_line_size=10000000, ignore_errors=true)
    ),
    repo_counts AS (
        SELECT 
            origin, 
            COUNT(*) AS total_files,
            SUM(is_ci_file) AS ci_files
        FROM ci_flags
        GROUP BY origin
    )
    SELECT f.*
    FROM ci_flags f
    JOIN repo_counts rc ON f.origin = rc.origin
    WHERE rc.total_files = rc.ci_files
""").fetchdf()

end = time.time()

print("time taken: ", end-start) # 34min

with open('../results/results_CI_v3.pkl', 'wb') as f:
    pickle.dump(ci_only_repos, f)
#25min
