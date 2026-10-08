import os
import pandas as pd
import glob
from pathlib import Path
import concurrent.futures
import tqdm

def process_single_file(csv_file):
    """Process a single CSV file and extract unique revisions"""
    try:
        # Read the CSV file
        df = pd.read_csv(csv_file, delimiter=';')
        
        # Check if 'revision' column exists
        if 'missing_commit' not in df.columns:
            return [], f"Warning: 'revision' column not found in {os.path.basename(csv_file)}"
        
        # Extract unique revision values
        file_revisions = set(df['revision'].unique())
        
        return file_revisions, None
    
    except Exception as e:
        return [], f"Error processing {os.path.basename(csv_file)}: {str(e)}"

def process_csv_revisions_parallel(directory_path, num_workers=14):
    """
    Process all CSV files in a directory in parallel to extract unique revision values.
    
    Args:
        directory_path (str): Path to the directory containing CSV files
        num_workers (int): Number of parallel workers
    
    Returns:
        tuple: (unique_revisions, revision_counts)
    """
    # Get all CSV files in the directory
    csv_files = glob.glob(os.path.join(directory_path, "*.csv"))
    
    if not csv_files:
        print(f"No CSV files found in {directory_path}")
        return [], None
    
    print(f"Found {len(csv_files)} CSV files. Processing with {num_workers} workers...")
    
    # Initialize an empty set for all revisions
    all_revisions = set()
    
    # Process files in parallel
    with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
        # Use tqdm to show progress
        results = list(tqdm.tqdm(
            executor.map(process_single_file, csv_files),
            total=len(csv_files),
            desc="Processing CSV files"
        ))
    
    # Process results
    for file_revisions, error_msg in results:
        if error_msg:
            print(error_msg)
        all_revisions.update(file_revisions)
    
    # Convert set to list
    unique_revisions = list(all_revisions)
    
    # Create a DataFrame with counts
    revision_counts = pd.DataFrame({
        'revision': unique_revisions,
        'count': [1] * len(unique_revisions)
    })
    
    # Display summary
    print(f"\nTotal unique revisions across all files: {len(unique_revisions):,}")
    
    return unique_revisions, revision_counts

# Example usage
if __name__ == "__main__":
    # Replace with your directory path
    directory = "/home/infres/rapaport/results/FULL_2024_08"  # Update this path
    
    # Process the files with 14 workers
    unique_revs, rev_counts = process_csv_revisions_parallel(directory, num_workers=14)
    
    # Save results if needed
    if rev_counts is not None and len(unique_revs) > 0:
        output_dir = "../results"
        rev_counts.to_csv(f"{output_dir}/revision_counts.csv", index=False)
        
        print(f"Results saved to {output_dir}/")