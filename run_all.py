#!/usr/bin/env python3
"""
Master script to run all Polymarket data download and processing scripts in the correct order.

This script orchestrates the entire data pipeline:
1. Download markets
2. Download event details
3. Process markets (Task 1: create individual market JSON files)
4. Download price history
5. Process data (Tasks 2 & 3: create TSV files)
6. (Optional) Analyze price data
7. (Optional) Filter price data

All outputs, errors, and logs are captured and saved to a comprehensive log file.
"""

import subprocess
import sys
import argparse
import logging
import os
from pathlib import Path
from datetime import datetime
import traceback

# --- Constants ---
DEFAULT_MARKET_DATA_DIR = "market_data"
DEFAULT_EVENT_DETAILS_DIR = "event_details"
DEFAULT_MARKET_DETAILS_DIR = "market_details"
DEFAULT_PRICE_HISTORY_DIR = "price_history"
DEFAULT_TIMESERIES_DIR = "timeseries_data"
DEFAULT_MARKET_TSV = "polymarket_markets.tsv"
DEFAULT_EVENT_TSV = "polymarket_events.tsv"
DEFAULT_STATUS = "closed"
DEFAULT_WORKERS = 10
DEFAULT_LOG_DIR = "logs"

# --- Helper Functions ---
def setup_logging(log_file_path):
    """Configures logging to both console and file."""
    log_formatter = logging.Formatter(
        '%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Prevent adding handlers multiple times
    if root_logger.hasHandlers():
        root_logger.handlers.clear()

    # File Handler
    file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
    file_handler.setFormatter(log_formatter)
    root_logger.addHandler(file_handler)

    # Console Handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    root_logger.addHandler(console_handler)

def run_script(script_name, args_list, description, log_file_path):
    """
    Runs a Python script and captures all output (stdout and stderr).
    
    Args:
        script_name: Name of the script to run (e.g., 'download_markets.py')
        args_list: List of command-line arguments
        description: Human-readable description of what the script does
        log_file_path: Path to the master log file
    
    Returns:
        tuple: (success: bool, return_code: int, stdout: str, stderr: str)
    """
    logger = logging.getLogger(script_name)
    logger.info("=" * 80)
    logger.info(f"Starting: {description}")
    logger.info(f"Command: python {script_name} {' '.join(args_list)}")
    logger.info("=" * 80)
    
    script_path = Path(__file__).parent / script_name
    if not script_path.exists():
        logger.error(f"Script not found: {script_path}")
        return False, -1, "", f"Script not found: {script_path}"
    
    try:
        # Run the script and capture both stdout and stderr
        result = subprocess.run(
            [sys.executable, str(script_path)] + args_list,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace'  # Handle encoding errors gracefully
        )
        
        # Log the output
        if result.stdout:
            logger.info("--- STDOUT ---")
            for line in result.stdout.splitlines():
                logger.info(line)
        
        if result.stderr:
            logger.warning("--- STDERR ---")
            for line in result.stderr.splitlines():
                logger.warning(line)
        
        # Log the return code
        if result.returncode == 0:
            logger.info(f"✓ Successfully completed: {description} (exit code: {result.returncode})")
        else:
            logger.error(f"✗ Failed: {description} (exit code: {result.returncode})")
        
        logger.info("=" * 80)
        
        return result.returncode == 0, result.returncode, result.stdout, result.stderr
        
    except Exception as e:
        error_msg = f"Exception while running {script_name}: {str(e)}\n{traceback.format_exc()}"
        logger.error(error_msg)
        return False, -1, "", error_msg

def main():
    parser = argparse.ArgumentParser(
        description="Run all Polymarket data download and processing scripts in order.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Example usage:
  python run_all.py --market-data-dir market_data --status closed
  
All scripts will run in sequence with full logging. The master log file will
contain all output from each script, including stdout and stderr.
        """
    )
    
    # Directory arguments
    parser.add_argument("--market-data-dir", type=str, default=DEFAULT_MARKET_DATA_DIR,
                        help=f"Directory for market data JSONL files (default: {DEFAULT_MARKET_DATA_DIR})")
    parser.add_argument("--event-details-dir", type=str, default=DEFAULT_EVENT_DETAILS_DIR,
                        help=f"Directory for event detail JSON files (default: {DEFAULT_EVENT_DETAILS_DIR})")
    parser.add_argument("--market-details-dir", type=str, default=DEFAULT_MARKET_DETAILS_DIR,
                        help=f"Directory for individual market JSON files (default: {DEFAULT_MARKET_DETAILS_DIR})")
    parser.add_argument("--price-history-dir", type=str, default=DEFAULT_PRICE_HISTORY_DIR,
                        help=f"Directory for price history JSON files (default: {DEFAULT_PRICE_HISTORY_DIR})")
    parser.add_argument("--timeseries-dir", type=str, default=DEFAULT_TIMESERIES_DIR,
                        help=f"Directory for timeseries TSV files (default: {DEFAULT_TIMESERIES_DIR})")
    
    # Output file arguments
    parser.add_argument("--market-tsv", type=str, default=DEFAULT_MARKET_TSV,
                        help=f"Output path for markets TSV file (default: {DEFAULT_MARKET_TSV})")
    parser.add_argument("--event-tsv", type=str, default=DEFAULT_EVENT_TSV,
                        help=f"Output path for events TSV file (default: {DEFAULT_EVENT_TSV})")
    
    # Script behavior arguments
    parser.add_argument("--status", type=str, default=DEFAULT_STATUS,
                        choices=['closed', 'open', 'all'],
                        help=f"Market status to download (default: {DEFAULT_STATUS})")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS,
                        help=f"Number of parallel workers for downloads (default: {DEFAULT_WORKERS})")
    parser.add_argument("--log-dir", type=str, default=DEFAULT_LOG_DIR,
                        help=f"Directory for log files (default: {DEFAULT_LOG_DIR})")
    parser.add_argument("--log-file", type=str,
                        help="Name of the master log file (default: run_all_YYYYMMDD_HHMMSS.log)")
    
    # Skip options
    parser.add_argument("--skip-download-markets", action="store_true",
                        help="Skip downloading markets (assume market_data_dir already exists)")
    parser.add_argument("--skip-download-events", action="store_true",
                        help="Skip downloading event details (assume event_details_dir already exists)")
    parser.add_argument("--skip-download-prices", action="store_true",
                        help="Skip downloading price history (assume price_history_dir already exists)")
    parser.add_argument("--skip-process-task1", action="store_true",
                        help="Skip Task 1 of process_data.py (assume market_details_dir already exists)")
    parser.add_argument("--skip-process-task2", action="store_true",
                        help="Skip Task 2 of process_data.py (TSV creation)")
    parser.add_argument("--skip-process-task3", action="store_true",
                        help="Skip Task 3 of process_data.py (timeseries TSV creation)")
    parser.add_argument("--skip-analyze", action="store_true",
                        help="Skip analyzing price data")
    parser.add_argument("--skip-filter", action="store_true",
                        help="Skip filtering price data")
    
    args = parser.parse_args()
    
    # Create log directory
    log_dir = Path(args.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    
    # Create master log file name with timestamp
    if args.log_file:
        master_log_file = log_dir / args.log_file
    else:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        master_log_file = log_dir / f"run_all_{timestamp}.log"
    
    # Setup logging
    setup_logging(str(master_log_file))
    logger = logging.getLogger("run_all")
    
    logger.info("=" * 80)
    logger.info("POLYMARKET DATA PIPELINE - MASTER SCRIPT")
    logger.info("=" * 80)
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"Master log file: {master_log_file}")
    logger.info(f"Arguments: {vars(args)}")
    logger.info("=" * 80)
    
    # Track overall success
    all_success = True
    step_results = []
    
    # Step 1: Download Markets
    if not args.skip_download_markets:
        success, return_code, stdout, stderr = run_script(
            "download_markets.py",
            [
                "--output-dir", args.market_data_dir,
                "--status", args.status,
                "--log-file", str(log_dir / "download_markets.log")
            ],
            "Download Markets",
            str(master_log_file)
        )
        all_success = all_success and success
        step_results.append(("Download Markets", success, return_code))
        if not success:
            logger.error("Step 1 (Download Markets) failed. Continuing with remaining steps...")
    else:
        logger.info("Skipping: Download Markets")
        step_results.append(("Download Markets", None, None))
    
    # Step 2: Download Event Details
    if not args.skip_download_events:
        success, return_code, stdout, stderr = run_script(
            "download_event_details.py",
            [
                "--market-data-dir", args.market_data_dir,
                "--output-dir", args.event_details_dir,
                "--workers", str(args.workers),
                "--log-file", str(log_dir / "download_event_details.log")
            ],
            "Download Event Details",
            str(master_log_file)
        )
        all_success = all_success and success
        step_results.append(("Download Event Details", success, return_code))
        if not success:
            logger.error("Step 2 (Download Event Details) failed. Continuing with remaining steps...")
    else:
        logger.info("Skipping: Download Event Details")
        step_results.append(("Download Event Details", None, None))
    
    # Step 3: Process Data - Task 1 (Create Individual Market JSONs)
    # This is needed before downloading price history
    if not args.skip_process_task1:
        success, return_code, stdout, stderr = run_script(
            "process_data.py",
            [
                "--market-data-dir", args.market_data_dir,
                "--event-details-dir", args.event_details_dir,
                "--market-output-dir", args.market_details_dir,
                "--skip-task2",
                "--skip-task3",
                "--log-file", str(log_dir / "process_data_task1.log")
            ],
            "Process Data - Task 1 (Individual Market JSONs)",
            str(master_log_file)
        )
        all_success = all_success and success
        step_results.append(("Process Data - Task 1", success, return_code))
        if not success:
            logger.error("Step 3 (Process Data - Task 1) failed. Cannot proceed with price history download.")
            logger.error("You may need to run this step manually before continuing.")
    else:
        logger.info("Skipping: Process Data - Task 1")
        step_results.append(("Process Data - Task 1", None, None))
    
    # Step 4: Download Price History
    if not args.skip_download_prices:
        success, return_code, stdout, stderr = run_script(
            "download_price_history.py",
            [
                "--market-details-dir", args.market_details_dir,
                "--output-dir", args.price_history_dir,
                "--workers", str(args.workers),
                "--log-file", str(log_dir / "download_price_history.log")
            ],
            "Download Price History",
            str(master_log_file)
        )
        all_success = all_success and success
        step_results.append(("Download Price History", success, return_code))
        if not success:
            logger.error("Step 4 (Download Price History) failed. Continuing with remaining steps...")
    else:
        logger.info("Skipping: Download Price History")
        step_results.append(("Download Price History", None, None))
    
    # Step 5: Process Data - Tasks 2 & 3 (Create TSV Files)
    if not args.skip_process_task2 or not args.skip_process_task3:
        process_args = [
            "--market-data-dir", args.market_data_dir,
            "--event-details-dir", args.event_details_dir,
            "--price-history-dir", args.price_history_dir,
            "--log-file", str(log_dir / "process_data_tasks23.log")
        ]
        
        if args.skip_process_task1:
            process_args.append("--skip-task1")
        else:
            process_args.extend(["--market-output-dir", args.market_details_dir])
        
        if not args.skip_process_task2:
            process_args.extend([
                "--market-tsv-output", args.market_tsv,
                "--event-tsv-output", args.event_tsv
            ])
        else:
            process_args.append("--skip-task2")
        
        if not args.skip_process_task3:
            process_args.extend(["--timeseries-output-dir", args.timeseries_dir])
        else:
            process_args.append("--skip-task3")
        
        success, return_code, stdout, stderr = run_script(
            "process_data.py",
            process_args,
            "Process Data - Tasks 2 & 3 (TSV Files)",
            str(master_log_file)
        )
        all_success = all_success and success
        step_results.append(("Process Data - Tasks 2 & 3", success, return_code))
        if not success:
            logger.error("Step 5 (Process Data - Tasks 2 & 3) failed.")
    else:
        logger.info("Skipping: Process Data - Tasks 2 & 3")
        step_results.append(("Process Data - Tasks 2 & 3", None, None))
    
    # Step 6: Analyze Price Data (Optional)
    if not args.skip_analyze:
        success, return_code, stdout, stderr = run_script(
            "analyze_price_data.py",
            [],
            "Analyze Price Data",
            str(master_log_file)
        )
        # Don't fail the entire pipeline if analysis fails
        step_results.append(("Analyze Price Data", success, return_code))
        if not success:
            logger.warning("Step 6 (Analyze Price Data) failed, but this is optional.")
    else:
        logger.info("Skipping: Analyze Price Data")
        step_results.append(("Analyze Price Data", None, None))
    
    # Step 7: Filter Price Data (Optional)
    if not args.skip_filter:
        success, return_code, stdout, stderr = run_script(
            "filter_price_data.py",
            [],
            "Filter Price Data",
            str(master_log_file)
        )
        # Don't fail the entire pipeline if filtering fails
        step_results.append(("Filter Price Data", success, return_code))
        if not success:
            logger.warning("Step 7 (Filter Price Data) failed, but this is optional.")
    else:
        logger.info("Skipping: Filter Price Data")
        step_results.append(("Filter Price Data", None, None))
    
    # Final Summary
    logger.info("=" * 80)
    logger.info("PIPELINE EXECUTION SUMMARY")
    logger.info("=" * 80)
    logger.info(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("")
    logger.info("Step Results:")
    for step_name, success, return_code in step_results:
        if success is None:
            status = "SKIPPED"
        elif success:
            status = "✓ SUCCESS"
        else:
            status = f"✗ FAILED (exit code: {return_code})"
        logger.info(f"  {step_name:40s} {status}")
    logger.info("")
    logger.info(f"Overall Status: {'✓ ALL STEPS COMPLETED SUCCESSFULLY' if all_success else '✗ SOME STEPS FAILED'}")
    logger.info(f"Master log file: {master_log_file}")
    logger.info("=" * 80)
    
    # Exit with appropriate code
    sys.exit(0 if all_success else 1)

if __name__ == "__main__":
    main()

