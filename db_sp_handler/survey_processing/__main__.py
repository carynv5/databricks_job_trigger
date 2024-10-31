"""
The entry point of the Python Wheel
"""
import argparse
from datetime import datetime

def main():
    """
    Main function to process survey data
    """
    parser = argparse.ArgumentParser(description='Process survey data')
    parser.add_argument('--date', type=str, required=True,
                      help='Processing date (YYYY-MM-DD)')
    parser.add_argument('--region', type=str, required=True,
                      help='Region code')
    parser.add_argument('--output-format', type=str, default='parquet',
                      help='Output format (parquet/csv)')
    
    args = parser.parse_args()
    print(f"Processing survey data for {args.date} in region {args.region} with format {args.output_format}")

if __name__ == '__main__':
    main()
