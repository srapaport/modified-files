import pandas as pd
import argparse
from typing import Dict, List, Tuple, Optional
import os


LICENSE_CATEGORIES = {
    # Permissive licenses with minimal restrictions
    "permissive": [
        "mit", "mit-0", "bsd-simplified", "bsd-new", "bsd-zero", "isc", "apache-2.0",
        "unlicense", "x11", "zlib", "zlib-acknowledgement", "clear-bsd",
        "json", "bsl-1.1", "public-domain", "bsd-original-uc", "python-2.0.1",
        "python", "openssl-ssleay", "ijg", "uoi-ncsa", "artistic-2.0",
        "amazon-sl", "bsd-plus-patent", "epl-1.0", "epl-2.0", "cc0-1.0"
    ],
    
    # Weak copyleft licenses
    "weak_copyleft": [
        "lgpl-2.0", "lgpl-2.1", "lgpl-2.1-plus", "lgpl-3.0", "mpl-2.0", "epl-1.0", 
        "epl-2.0", "eupl-1.2"
    ],
    
    # Creative Commons licenses (except "cc0-1.0")
    "cc-by": [
        "cc-by-3.0", "cc-by-4.0"
    ],
    
    "cc-by-sa": [
        "cc-by-sa-2.5", "cc-by-sa-4.0"
    ],
    
    "cc-by-nc": [
        "cc-by-nc-4.0"
    ],
    
    "cc-by-nc-sa": [
        "cc-by-nc-sa-4.0"
    ],
    
    # Strong copyleft licenses
    "strong_copyleft": [
        "gpl-1.0-plus", "gpl-2.0", "gpl-2.0-plus", "gpl-3.0", "gpl-3.0-plus", 
        "agpl-3.0"
    ],
    
    # Special/Unusual licenses
    "special": [
        "ofl-1.1", "wtfpl-2.0", "hippocratic-1.2", "hippocratic-2.1",
        "stable-diffusion-2022-08-22", "tanuki-community-sla-1.3",
        "gfdl-1.3", "qpl-1.0", "artistic-perl-1.0", "openpub",
        "unknown-license-reference", "proprietary-license"
    ],

    # License with exceptions
    "exceptions": [
        "apache-2.0 with llvm-exception", "gpl-2.0 with classpath-exception-2.0",
        "gpl-3.0 with gcc-exception-3.1", "classpath-exception-2.0",
        "gpl-2.0-plus with geoserver-exception-2.0-plus", "nvidia-2002",
        "gcc-exception-3.1"
    ]
}


def get_license_category(license_name):
    """
    Determine the category of a given license.
    
    Args:
        license_name: Name of the license
        
    Returns:
        Category of the license as a string
    """
    license_name = license_name.lower()
    for category, licenses in LICENSE_CATEGORIES.items():
        if license_name in licenses:
            return category
            
    if " and " in license_name or " or " in license_name:
        return "compound"
    if " with " in license_name:
        return "exceptions"
    return "unknown"


def get_precaution_level(from_license, to_license):
    """
    Determine the precaution level based on license changes.
    
    Args:
        from_license: Original license
        to_license: New license
        
    Returns:
        Precaution level as a string: Low, Medium, High, or "High - Needs Legal Review"
    """
    from_category = get_license_category(from_license)
    to_category = get_license_category(to_license)
    
    special_categories = ["special", "unknown", "compound", "exceptions"]
    if from_category in special_categories or to_category in special_categories:
        return "High - Needs Legal Review"
    
    if (
        (from_category == "permissive" and 
         to_category in ["weak_copyleft", "strong_copyleft", "cc-by", "cc-by-sa", "cc-by-nc", "cc-by-nc-sa"]) or
        (from_category == "cc-by" and 
         to_category in ["weak_copyleft", "strong_copyleft", "cc-by-sa", "cc-by-nc", "cc-by-nc-sa"]) or
        (from_category in ["cc-by-sa", "cc-by-nc"] and 
         to_category in ["strong_copyleft", "cc-by-nc-sa"]) or
        (from_category == "cc-by-nc-sa" and to_category == "strong_copyleft") or
        (from_category == "weak_copyleft" and to_category == "strong_copyleft")
    ):
        return "High"
    
    if (
        (from_category == "strong_copyleft" and 
         to_category in ["weak_copyleft", "cc-by-nc", "cc-by-sa", "cc-by", "permissive"]) or
        (from_category in ["weak_copyleft", "cc-by-nc", "cc-by-sa"] and 
         to_category in ["cc-by", "permissive"]) or
        (from_category != "permissive" and to_category == "permissive")
    ):
        return "Medium"
    
    if from_category == to_category:
        return "Low"

    return "Undetermined"


def analyze_license_changes(
    input_file,
    from_col = None,
    to_col = None
):
    """
    Analyze license changes from an Excel file and categorize them.
    
    Args:
        input_file: Path to Excel file with license data
        from_col: Column name for original licenses (optional)
        to_col: Column name for new licenses (optional)
        
    Returns:
        DataFrame with analysis results
    """
    file_ext = os.path.splitext(input_file)[1].lower()
    if file_ext == '.csv':
        df = pd.read_csv(input_file)
    else:
        df = pd.read_excel(input_file)
    
    if from_col is None or to_col is None:
        columns = df.columns.tolist()
        if len(columns) < 2:
            raise ValueError("Input file must have at least two columns")
        from_col = columns[0]
        to_col = columns[1]
    
    results = pd.DataFrame({
        'from_license': df[from_col],
        'to_license': df[to_col],
        'from_category': [get_license_category(license) for license in df[from_col]],
        'to_category': [get_license_category(license) for license in df[to_col]]
    })
    
    results['precaution_level'] = [
        get_precaution_level(row['from_license'], row['to_license']) 
        for _, row in results.iterrows()
    ]
    return results


def generate_summary(results):
    """
    Generate a summary of the analysis results.
    
    Args:
        results: DataFrame with analysis results
        
    Returns:
        Dictionary with summary statistics
    """
    precaution_counts = results['precaution_level'].value_counts().to_dict()
    
    total = len(results)
    precaution_percentages = {
        level: f"{(count / total * 100):.1f}%" 
        for level, count in precaution_counts.items()
    }
    
    examples = {}
    for level in precaution_counts.keys():
        level_examples = results[results['precaution_level'] == level].head(3)
        examples[level] = [
            f"{row['from_license']} → {row['to_license']}" 
            for _, row in level_examples.iterrows()
        ]
    
    return {
        'total_changes': total,
        'counts': precaution_counts,
        'percentages': precaution_percentages,
        'examples': examples
    }


def main():
    parser = argparse.ArgumentParser(
        description='Analyze open-source license changes and categorize by precaution level.'
    )
    parser.add_argument('input_file', help='Path to Excel or CSV file with license data')
    parser.add_argument(
        '-f', '--from-column', 
        help='Column name for original licenses (defaults to first column)'
    )
    parser.add_argument(
        '-t', '--to-column', 
        help='Column name for new licenses (defaults to second column)'
    )
    parser.add_argument(
        '-o', '--output', 
        help='Output Excel file path (optional)'
    )
    parser.add_argument(
        '-s', '--summary-only', 
        action='store_true',
        help='Only display summary without full results'
    )
    
    args = parser.parse_args()
    
    results = analyze_license_changes(
        args.input_file,
        args.from_column,
        args.to_column
    )
    
    summary = generate_summary(results)
    
    print("\n=== LICENSE CHANGE ANALYSIS SUMMARY ===\n")
    print(f"Total changes analyzed: {summary['total_changes']}\n")
    
    print("Precaution Level Breakdown:")
    for level, count in summary['counts'].items():
        print(f"  {level}: {count} changes ({summary['percentages'][level]})")
    
    print("\nExamples by Precaution Level:")
    for level, examples in summary['examples'].items():
        print(f"  {level}:")
        for example in examples:
            print(f"    - {example}")
    
    if not args.summary_only:
        print("\n=== FULL ANALYSIS RESULTS ===\n")
        pd.set_option('display.max_rows', None)
        print(results)
    
    if args.output:
        results.to_excel(args.output, index=False)
        print(f"\nFull results saved to {args.output}")


if __name__ == "__main__":
    main()