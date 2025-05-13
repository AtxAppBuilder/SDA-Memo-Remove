import os
import requests
import time
import re
import logging
import dropbox
from concurrent.futures import ThreadPoolExecutor
from dropbox.exceptions import ApiError, AuthError
from dotenv import load_dotenv
from dropbox.files import DeleteArg, FileMetadata, FolderMetadata
import unicodedata
from typing import List, Dict, Optional  

# ======================
# TURBO CONFIGURATION
# ======================
TARGET_FOLDER = "/$ JLR DATA MIGRATION/David"
EXCLUDE_FOLDERS = {
    "/$ JLR DATA MIGRATION/Archive",
    "/$ JLR DATA MIGRATION/David/Archive",
    "/Archive"
}

# ======================
# ENHANCED CONFIGURATION
# ======================
MAX_RETRIES = 5  # Number of retry attempts
INITIAL_TIMEOUT = 60  # Base timeout in seconds
BACKOFF_FACTOR = 1.5  # Exponential backoff multiplier
DRY_RUN = True  # Set to True for testing
MAX_THREADS = 4  # Optimal for Dropbox API
API_DELAY = 1.0  # Minimum safe delay (seconds)
BATCH_SIZE = 100  # Dropbox maximum

# ======================
# ROBUST DROPBOX CLIENT
# ======================
def create_dropbox_client():
    return dropbox.Dropbox(
        os.getenv("DROPBOX_ACCESS_TOKEN"),
        timeout=INITIAL_TIMEOUT,  # This sets GLOBAL timeout
        max_retries_on_error=MAX_RETRIES
    )

# ======================
# RETRY MECHANISM
# ======================
def robust_api_call(func, *args, **kwargs):
    """Handles timeouts with exponential backoff"""
    last_error = None
    current_timeout = INITIAL_TIMEOUT
    
    for attempt in range(MAX_RETRIES + 1):
        try:
            return func(*args, **kwargs)
        except (requests.exceptions.ReadTimeout, 
                requests.exceptions.ConnectionError) as e:
            last_error = e
            if attempt < MAX_RETRIES:
                wait_time = current_timeout * BACKOFF_FACTOR
                logger.warning(f"Timeout (attempt {attempt + 1}), retrying in {wait_time:.1f}s...")
                time.sleep(wait_time)
                current_timeout *= BACKOFF_FACTOR
            continue
    
    logger.error(f"Max retries exceeded")
    raise last_error


# ======================
# BULLETPROOF LOGGING
# ======================
logging.basicConfig(
    filename='jlr_memo_cleanup_turbo.log',
    level=logging.INFO,
    format='%(asctime)s.%(msecs)03d - %(levelname)s - %(message)s',
    datefmt='%H:%M:%S',
    filemode='w'
)
logger = logging.getLogger('TurboCleaner')
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logger.addHandler(console)

# ======================
# HYPER-EFFICIENT MATCHING
# ======================
def is_target_file(filename: str) -> bool:
    """Lightning-fast matching with compiled regex"""
    patterns = [
        re.compile(r'memo[\s\-_]*style\.pdf$', re.IGNORECASE),
        re.compile(r'memostyle\.pdf$', re.IGNORECASE),
        re.compile(r'memo[\s\-_]*style.*\.pdf$', re.IGNORECASE)
    ]
    return any(pattern.search(filename) for pattern in patterns)

# ======================
# PARALLEL SCANNING
# ======================
def scan_target_folder(dbx: dropbox.Dropbox) -> Dict[str, List[str]]:
    """Robust folder scanning with timeout protection"""
    results = {'target_files': [], 'other_files': [], 'failed_folders': []}
    cursor = None
    processed_entries = 0
    retry_count = 0
    MAX_RETRIES = 3
    
    try:
        while True:
            try:
                # Initial scan or continuation
                if cursor is None:
                    response = robust_api_call(
                        dbx.files_list_folder,
                        TARGET_FOLDER,
                        recursive=True,
                        include_non_downloadable_files=False,
                        limit=2000  # Larger page size
                    )
                else:
                    response = robust_api_call(
                        dbx.files_list_folder_continue,
                        cursor
                    )
                
                # Process entries
                for entry in response.entries:
                    processed_entries += 1
                    
                    # Progress tracking
                    if processed_entries % 500 == 0:
                        logger.info(f"Scanned {processed_entries} entries...")
                        print(f"\rScanned: {processed_entries}", end='', flush=True)
                    
                    # Skip excluded folders
                    if (isinstance(entry, FolderMetadata) and 
                        any(entry.path_lower.startswith(excl.lower()) for excl in EXCLUDE_FOLDERS)):
                        continue
                        
                    # Target file check
                    if isinstance(entry, FileMetadata) and is_target_file(entry.name):
                        results['target_files'].append(entry.path_lower)
                
                # Check for completion
                if not response.has_more:
                    break
                    
                cursor = response.cursor
                time.sleep(API_DELAY)
                retry_count = 0  # Reset on success
                
            except (ApiError, requests.exceptions.RequestException) as e:
                if retry_count < MAX_RETRIES:
                    retry_count += 1
                    wait_time = API_DELAY * (2 ** retry_count)  # Exponential backoff
                    logger.warning(f"Retry {retry_count}/{MAX_RETRIES} after {wait_time:.1f}s")
                    time.sleep(wait_time)
                    continue
                raise
            
    except Exception as e:
        logger.error(f"Fatal scan error after {processed_entries} entries: {str(e)}")
        results['failed_folders'].append(cursor or TARGET_FOLDER)
        if not results['target_files']:  # Only raise if no results at all
            raise
    
    logger.info(f"Scan completed: {processed_entries} entries processed")
    print(f"\nScan found {len(results['target_files'])} target files")
    return results

# ======================
# NUCLEAR DELETION
# ======================
def turbo_delete_files(dbx: dropbox.Dropbox, file_paths: List[str]) -> None:
    """Parallel batch deletion with retries"""
    def delete_batch(batch: List[str]) -> Optional[List[str]]:
        time.sleep(API_DELAY)
        try:
            if not DRY_RUN:
                dbx.files_delete_batch([DeleteArg(path=p) for p in batch])
            logger.info(f"Processed batch ({len(batch)} files)")
            return None
        except ApiError as e:
            logger.warning(f"Batch failed (retrying): {str(e)}")
            return batch  # Return failed items
    
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        # Split into batches
        batches = [file_paths[i:i + BATCH_SIZE] 
                 for i in range(0, len(file_paths), BATCH_SIZE)]
        
        # Initial parallel processing
        failed_batches = list(filter(None, executor.map(delete_batch, batches)))
        
        # Retry failed batches sequentially
        for batch in failed_batches:
            time.sleep(API_DELAY * 2)  # Extra delay for retries
            delete_batch(batch)

# ======================
# INTELLIGENT FOLDER CLEANUP
# ======================
def clean_empty_folders(dbx: dropbox.Dropbox, target_files: List[str]) -> None:
    """New robust folder deletion with retries"""
    def should_delete(folder_path: str) -> bool:
        try:
            response = dbx.files_list_folder(folder_path)
            return all(is_target_file(entry.name) 
                     for entry in response.entries
                     if isinstance(entry, FileMetadata))
        except Exception as e:
            logger.warning(f"Check failed for {folder_path}: {str(e)}")
            return False

    folders_to_check = set()
    # Build folder list
    for file_path in target_files:
        folder = os.path.dirname(file_path)
        while len(folder) > len(TARGET_FOLDER):
            folders_to_check.add(folder)
            folder = os.path.dirname(folder)

    # Process folders deepest first
    for folder in sorted(folders_to_check, key=lambda x: -len(x.split('/'))):
        retry_count = 0
        while retry_count < MAX_RETRIES:
            try:
                if not any(folder.startswith(excl) for excl in EXCLUDE_FOLDERS):
                    if should_delete(folder):
                        if DRY_RUN:
                            logger.info(f"[DRY RUN] Would delete: {folder}")
                        else:
                            dbx.files_delete_v2(folder) 
                            logger.info(f"Deleted folder: {folder}")
                break  # Success - move to next folder
                
            except (requests.exceptions.ReadTimeout, 
                   dropbox.exceptions.ApiError) as e:
                retry_count += 1
                wait = min(30, (2 ** retry_count))  # Cap wait time at 30s
                logger.warning(f"Retry {retry_count}/{MAX_RETRIES} for {folder}")
                time.sleep(wait)
                if retry_count == MAX_RETRIES:
                    logger.error(f"Permanently failed: {folder}")

# ======================
# MAIN EXECUTION
# ======================
def main():
    logger.info("=== TURBO MEMO CLEANUP STARTED ===")
    print(f"Target: {TARGET_FOLDER}")
    print(f"Excluding: {', '.join(EXCLUDE_FOLDERS)}")
    print(f"Mode: {'DRY RUN' if DRY_RUN else 'LIVE'} | Threads: {MAX_THREADS}")
    
    # =============================================
    # CHANGED: New resilient client initialization
    # =============================================
    load_dotenv()
    dbx = create_dropbox_client()  # Using the new robust client
    
    retry_main = 0
    while retry_main < 3:  # NEW: Automatic recovery loop
        try:
            start_time = time.time()
            
            # 1. Scan (unchanged)
            scan_results = scan_target_folder(dbx)
            target_files = scan_results['target_files']
            
            if not target_files:
                print("No target files found")
                return
                
            # 2. Preview (unchanged)
            print(f"\nFound {len(target_files)} targets (sample):")
            for f in target_files[:3]:
                print(f"- {f}")
            if len(target_files) > 3:
                print(f"- Plus {len(target_files)-3} more")
            
            # 3. Confirm (unchanged)
            if not DRY_RUN and input("\nProceed with deletion? (y/n): ").lower() != 'y':
                print("Cancelled")
                return
                
            # 4. Delete files (unchanged)
            print("\nDeleting files...")
            turbo_delete_files(dbx, target_files)
            
            # 5. Clean folders (unchanged but now more resilient)
            if not DRY_RUN and input("\nClean empty folders? (y/n): ").lower() == 'y':
                print("Cleaning folders...")
                clean_empty_folders(dbx, target_files)
            
            break  # NEW: Exit retry loop on success
            
        # =============================================
        # NEW: Special handling for timeouts
        # =============================================
        except requests.exceptions.ReadTimeout:
            retry_main += 1
            print(f"\nConnection timeout (attempt {retry_main}/3). Reconnecting...")
            dbx = create_dropbox_client()  # Fresh connection
            time.sleep(10 * retry_main)  # Wait longer each time
            
        # =============================================
        # Existing error handling (unchanged)
        # =============================================
        except Exception as e:
            logger.critical(f"Fatal error: {str(e)}", exc_info=True)
            print(f"! Critical error: {str(e)}")
            break
            
        # =============================================
        # Existing cleanup (unchanged)
        # =============================================
        finally:
            dbx.close()
            elapsed = time.time() - start_time
            logger.info(f"Completed in {elapsed:.2f} seconds")
            print(f"\nDone in {elapsed:.2f}s. See log for details.")

if __name__ == "__main__":
    main()