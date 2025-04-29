import os
import sys
import multiprocessing
import shutil
from main_zhihu import ZhihuParser
from tqdm import tqdm
from functools import partial
from pathlib import Path

# Configuration
COOKIES_FILE = "cookies.txt"
MAX_PROCESSES = multiprocessing.cpu_count()

# Path configuration
BASE_DIR = Path("zhihu")
URLS_DIR = BASE_DIR / "urls"  # Directory containing URL lists
EXTRACTED_DIR = BASE_DIR / "markdown"  # Directory for extracted articles
TEMP_DIR = BASE_DIR / "temp_zips"  # Directory for temporary zip files

def setup_directories():
    """Create necessary directories if they don't exist."""
    os.makedirs(URLS_DIR, exist_ok=True)
    os.makedirs(EXTRACTED_DIR, exist_ok=True)
    os.makedirs(TEMP_DIR, exist_ok=True)

def get_url_files():
    """Get all .txt files from the urls directory."""
    return list(URLS_DIR.glob("*.txt"))

def read_urls_from_file(file_path):
    """Read URLs from a text file, one URL per line."""
    with open(file_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def read_cookies_from_file(file_path):
    """Read cookies from a text file."""
    if not os.path.exists(file_path):
        print(f"Error: Cookies file {file_path} does not exist")
        sys.exit(1)
    
    with open(file_path, 'r', encoding='utf-8') as f:
        cookies = f.read().strip()
        if not cookies:
            print("Error: Cookies file is empty")
            sys.exit(1)
        return cookies

def get_article_id(url):
    """Extract article ID from URL."""
    if 'zhuanlan.zhihu.com/p/' in url:
        return url.split('zhuanlan.zhihu.com/p/')[-1].split('/')[0]
    elif 'question' in url and 'answer' in url:
        return url.split('answer/')[-1].split('/')[0]
    elif 'zvideo' in url:
        return url.split('zvideo/')[-1].split('/')[0]
    elif 'column' in url:
        return url.split('column/')[-1].split('/')[0]
    return None

def article_exists(article_id, output_dir):
    """Check if article already exists in output directory."""
    for root, dirs, files in os.walk(output_dir):
        if article_id in root:
            return True
    return False

def process_single_url(url, cookies, output_dir):
    """Process a single URL using ZhihuParser."""
    article_id = get_article_id(url)
    if not article_id:
        return url, False, "Invalid URL format"

    if article_exists(article_id, output_dir):
        return url, True, "Article already exists"

    parser = ZhihuParser(cookies=cookies, keep_logs=False, temp_dir=str(TEMP_DIR), output_dir=str(output_dir))
    try:
        parser.judge_type(url)
        return url, True, None
    except Exception as e:
        return url, False, str(e)

def process_urls(urls, cookies, output_dir):
    """Process a list of URLs using ZhihuParser in parallel using multiprocessing."""
    success_count = 0
    failure_count = 0
    skipped_count = 0
    failed_urls = []

    # Create a partial function with cookies and output_dir pre-filled
    process_func = partial(process_single_url, cookies=cookies, output_dir=output_dir)

    # Create a pool of workers
    with multiprocessing.Pool(processes=MAX_PROCESSES) as pool:
        # Create a list of tasks
        tasks = pool.imap_unordered(process_func, urls)
        
        # Process results with progress bar
        with tqdm(total=len(urls), desc="Processing URLs", 
                 bar_format='{l_bar}{bar}| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, {rate_fmt}]') as pbar:
            for url, success, error in tasks:
                if success:
                    if error == "Article already exists":
                        skipped_count += 1
                        pbar.set_postfix({'Status': 'Skipped', 'Failed': failure_count})
                    else:
                        success_count += 1
                        pbar.set_postfix({'Status': 'Success', 'Failed': failure_count})
                else:
                    failure_count += 1
                    failed_urls.append((url, error))
                    pbar.set_postfix({'Status': 'Failed', 'Failed': failure_count})
                pbar.update(1)

    return success_count, failure_count, skipped_count, failed_urls

def cleanup_temp_files():
    """Clean up temporary zip files."""
    if TEMP_DIR.exists():
        shutil.rmtree(TEMP_DIR)
        os.makedirs(TEMP_DIR)

def process_url_file(url_file, cookies):
    """Process a single URL file."""
    # Create output directory based on URL file name (without extension)
    output_dir = EXTRACTED_DIR / url_file.stem
    os.makedirs(output_dir, exist_ok=True)

    print(f"\nProcessing URL file: {url_file.name}")
    print(f"Output directory: {output_dir}")

    urls = read_urls_from_file(url_file)
    print(f"Found {len(urls)} URLs to process")

    success_count, failure_count, skipped_count, failed_urls = process_urls(urls, cookies, output_dir)

    print("\nDownload Summary:")
    print(f"Total URLs processed: {len(urls)}")
    print(f"Successfully downloaded: {success_count}")
    print(f"Skipped (already exists): {skipped_count}")
    print(f"Failed downloads: {failure_count}")

    if failed_urls:
        print("\nFailed URLs:")
        for url, error in failed_urls:
            print(f"- {url}")
            print(f"  Error: {error}")

    return success_count, failure_count, skipped_count

def main():
    # Setup directories
    setup_directories()
    cleanup_temp_files()

    print("Reading cookies from file...")
    cookies = read_cookies_from_file(COOKIES_FILE)

    # Get all URL files
    url_files = get_url_files()
    if not url_files:
        print(f"Error: No URL files found in {URLS_DIR}")
        sys.exit(1)

    print(f"\nFound {len(url_files)} URL files to process")
    total_success = 0
    total_failure = 0
    total_skipped = 0

    for url_file in url_files:
        success, failure, skipped = process_url_file(url_file, cookies)
        total_success += success
        total_failure += failure
        total_skipped += skipped

    print("\nOverall Summary:")
    print(f"Total URL files processed: {len(url_files)}")
    print(f"Total articles successfully downloaded: {total_success}")
    print(f"Total articles skipped: {total_skipped}")
    print(f"Total articles failed: {total_failure}")

    # Clean up temporary files
    cleanup_temp_files()

if __name__ == "__main__":
    # Required for Windows multiprocessing
    multiprocessing.freeze_support()
    main() 