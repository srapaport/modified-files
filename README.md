# Altered History Analysis

A Rust application for analyzing altered file histories in software repositories using the Software Heritage graph infrastructure. This tool identifies and analyzes file modifications across different snapshots and branches to detect potential history tampering or unauthorized changes.

## 🔍 Overview

This project analyzes software repository histories by:
- Processing CSV files containing file modification events
- Comparing file states across different repository snapshots
- Using the Software Heritage graph to validate actual file changes
- Generating comprehensive reports on altered histories
- Storing results in PostgreSQL for further analysis

## ✨ Features

- **Parallel Processing**: Utilizes Rayon for concurrent analysis of multiple repositories
- **Graph Analysis**: Integrates with Software Heritage's comprehensive graph database
- **Database Integration**: Stores results in PostgreSQL with optimized bulk operations
- **Progress Tracking**: Real-time progress bars and detailed logging
- **Memory Optimization**: Adaptive batch sizing based on available system memory
- **Comprehensive Logging**: Rotating log files with configurable levels

## 🛠️ Prerequisites

- **Rust** (2021 edition or later)
- **PostgreSQL** database
- **Software Heritage Graph**: Access to compressed SWH graph data
- **System Requirements**: Minimum 1.5TB RAM (recommended for large graphs)

## 📦 Installation

1. **Install dependencies:**
   ```bash
   cargo build --release
   ```

2. **Set up environment variables:**
   Create a `.env` file in the project root:
   ```env
   DATABASE_URL=postgresql://username:password@localhost/database_name
   ```

3. **Configure PostgreSQL:**
   - Ensure PostgreSQL is running
   - Create a database for storing results
   - Grant appropriate permissions to the user

## ⚙️ Configuration

Before running the application, configure the following constants in `src/main.rs`:

```rust
const GRAPH_PATH: &str = "/path/to/compressed/graph"; // Path without extension
const RESULTS_PATH: &str = "/path/to/results/directory";
```

## 🚀 Usage

### Basic Usage

```bash
cargo run --release
```

## 📊 Output

### Generated Files

1. **CSV Reports**:
   - `results/modified_files.csv`: Detailed analysis of modified files
   - Contains modification status for each file (Modified, NotFound)

2. **Database Tables**:
   - `modified_files`: Processed file modification data
   - `altered_histories`: Aggregated analysis results

3. **Log Files**:
   - `logs/prod_*.log`: Application execution logs with rotation
   - Progress tracking and error reporting

### Output CSV Schema

```csv
origin,revision,branch,snapshot_without,path,status
https://github.com/user/repo,abc123,main,def456,src/file.rs,Modified
```

## 🏗️ Architecture

### Core Components

- **`main.rs`**: Application entry point and workflow orchestration
- **`lib.rs`**: Core analysis functions (`retrieve_file_modified`, `all_modified`)
- **`models.rs`**: Database operations and CSV conversion utilities
- **`file_modified.rs`**: Software Heritage graph analysis functions
- **`env.rs`**: Data structures and environment configuration

### Data Flow

1. **CSV Ingestion** → Parse input files for modification events
2. **Graph Analysis** → Query Software Heritage graph for actual file states
3. **Comparison** → Determine real vs. reported modifications
4. **Storage** → Save results to PostgreSQL database
5. **Reporting** → Generate analysis reports

## 🔧 Development

### Building from Source

```bash
# Release build (optimized)
cargo build --release
```
