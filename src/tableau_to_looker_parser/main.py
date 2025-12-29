"""
Tableau Workbook Download and Extraction Module

This module handles:
1. Downloading Tableau workbooks from the server OR processing local TWB files
2. Extracting XML content
3. Generating JSON output using MigrationEngine
"""

import sys
import zipfile
import time
import json
import requests
from pathlib import Path
import tableauserverclient as TSC
from slugify import slugify
from concurrent.futures import ThreadPoolExecutor, as_completed

from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.converter import transform_json


def fetch_site_luid(server_url: str, username: str, password: str, site_content_url: str) -> str:
    """
    Fetch the Tableau Site LUID from the Tableau Server API using the site content URL.
    
    Args:
        server_url: Tableau server URL (e.g., https://tableau.squareshift.dev)
        username: Tableau username
        password: Tableau password
        site_content_url: Site content URL (e.g., "dev", "prod")
        
    Returns:
        str: Site LUID (e.g., "fe224d5a-80e9-4641-aec9-565bd4199896")
        
    Raises:
        Exception: If the API request fails or LUID cannot be retrieved
    """
    # Construct the signin API endpoint
    api_version = "3.19"  # You can make this configurable if needed
    signin_url = f"{server_url.rstrip('/')}/api/{api_version}/auth/signin"
    
    # Prepare the request body
    payload = {
        "credentials": {
            "name": username,
            "password": password,
            "site": {
                "contentUrl": site_content_url
            }
        }
    }
    
    # Set headers
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }
    
    try:
        print(f"Fetching Site LUID for site '{site_content_url}' from Tableau Server...")
        response = requests.post(signin_url, json=payload, headers=headers, timeout=30)
        response.raise_for_status()
        
        response_data = response.json()
        
        # Extract the site LUID from the response
        site_luid = response_data.get("credentials", {}).get("site", {}).get("id")
        
        if not site_luid:
            raise ValueError(f"Site LUID not found in API response. Response: {response_data}")
        
        print(f"✅ Successfully fetched Site LUID: {site_luid}")
        return site_luid
        
    except requests.exceptions.RequestException as e:
        error_msg = f"Failed to fetch Site LUID from Tableau API: {e}"
        if hasattr(e, 'response') and e.response is not None:
            try:
                error_detail = e.response.json()
                error_msg += f"\n   API Error Details: {error_detail}"
            except:
                error_msg += f"\n   Response Status: {e.response.status_code}"
                error_msg += f"\n   Response Text: {e.response.text[:200]}"
        raise Exception(error_msg) from e
    except (KeyError, ValueError) as e:
        raise Exception(f"Failed to parse Site LUID from API response: {e}") from e


def validate_zip_file(file_path):
    """
    Validate that a zip file is complete and not corrupted.
    
    Args:
        file_path: Path to the zip file
        
    Returns:
        bool: True if valid, False otherwise
    """
    try:
        with zipfile.ZipFile(file_path, 'r') as z:
            # Test the zip file by trying to read the file list
            z.testzip()
            return True
    except (zipfile.BadZipFile, zipfile.LargeZipFile) as e:
        print(f"Invalid zip file {file_path}: {e}")
        return False
    except Exception as e:
        print(f"Error validating zip file {file_path}: {e}")
        return False


def convert_twbx_to_twb(twbx_path, output_path=None, remove_twbx=True):
    """
    Convert a .twbx (packaged workbook) file to .twb (workbook) file.
    
    Args:
        twbx_path: Path to the .twbx file
        output_path: Optional path for the output .twb file. If None, 
                     creates a .twb file in the same directory as the .twbx file
        remove_twbx: If True, delete the .twbx file after successful conversion (default: True)
        
    Returns:
        Path: Path to the created .twb file, or None if conversion fails
    """
    twbx_path = Path(twbx_path)
    
    if not twbx_path.exists():
        print(f"Error: .twbx file not found: {twbx_path}")
        return None
    
    if not twbx_path.suffix.lower() == '.twbx':
        print(f"Error: File is not a .twbx file: {twbx_path}")
        return None
    
    # Validate the zip file first
    if not validate_zip_file(twbx_path):
        print(f"Error: .twbx file is corrupted or incomplete: {twbx_path}")
        return None
    
    try:
        # Determine output path
        if output_path is None:
            output_path = twbx_path.with_suffix('.twb')
        else:
            output_path = Path(output_path)
        
        # Extract .twb from .twbx
        with zipfile.ZipFile(twbx_path, 'r') as z:
            twb_files = [f for f in z.namelist() if f.endswith('.twb')]
            if not twb_files:
                print(f"Error: No .twb file found inside .twbx: {twbx_path}")
                return None
            
            # Use the first .twb file found
            twb_file = twb_files[0]
            with z.open(twb_file) as source:
                # Write the .twb file
                output_path.write_bytes(source.read())
        
        # Remove .twbx file if requested
        if remove_twbx:
            try:
                twbx_path.unlink()
                print(f"✅ Converted {twbx_path.name} to {output_path.name} and removed .twbx file")
            except Exception as e:
                print(f"✅ Converted {twbx_path.name} to {output_path.name}")
                print(f"⚠️  Warning: Could not remove .twbx file: {e}")
        else:
            print(f"✅ Converted {twbx_path.name} to {output_path.name}")
        
        return output_path
        
    except Exception as e:
        print(f"Error converting .twbx to .twb: {e}")
        import traceback
        traceback.print_exc()
        return None


def extract_twb_xml(file_path):
    """
    Extract XML content from a Tableau workbook file (.twb or .twbx).
    
    Args:
        file_path: Path to the .twb or .twbx file
        
    Returns:
        str: XML content as string, or empty string if extraction fails
    """
    try:
        if zipfile.is_zipfile(file_path):
            # Validate zip file first
            if not validate_zip_file(file_path):
                return ""
            
            with zipfile.ZipFile(file_path, 'r') as z:
                twb_files = [f for f in z.namelist() if f.endswith('.twb')]
                if not twb_files:
                    return ""
                with z.open(twb_files[0]) as f:
                    return f.read().decode("utf-8", errors="ignore")
        else:
            return Path(file_path).read_text(encoding="utf-8", errors="ignore")
    except Exception as e:
        print(f"Failed to read {file_path}: {e}")
        return ""


def generate_json_from_twb(twb_file_path: str, output_dir: str = "output") -> dict:
    """
    Generate JSON from a local TWB file using MigrationEngine.
    
    Args:
        twb_file_path: Path to .twb or .twbx file
        output_dir: Directory to save JSON output
        
    Returns:
        dict: Migration result with statistics
    """
    twb_path = Path(twb_file_path)
    if not twb_path.exists():
        raise FileNotFoundError(f"TWB file not found: {twb_file_path}")
    
    if twb_path.suffix.lower() not in ['.twb', '.twbx']:
        raise ValueError(f"Invalid file type. Expected .twb or .twbx, got: {twb_path.suffix}")
    
    print(f"\n📄 Processing local TWB file: {twb_path.name}")
    print(f"   Path: {twb_path.absolute()}")
    
    # Initialize MigrationEngine
    print("\n🔧 Initializing MigrationEngine...")
    try:
        engine = MigrationEngine(use_v2_parser=True)
        print("✅ MigrationEngine initialized")
    except Exception as e:
        print(f"❌ Error initializing engine: {e}")
        raise
    
    # Generate JSON
    print(f"\n🚀 Generating JSON from TWB file...")
    try:
        result = engine.migrate_file(str(twb_path), output_dir)
        print("✅ JSON generation completed successfully")
        
        # Display summary
        output_path = Path(output_dir) / "processed_pipeline_output.json"
        if output_path.exists():
            print(f"\n📊 Generated JSON file: {output_path}")
            print(f"   File size: {output_path.stat().st_size / 1024:.2f} KB")
            
            # Transform the JSON to extract only specified fields
            print(f"\n🔄 Transforming JSON to extract specified fields...")
            try:
                transform_json(str(output_path), str(output_path), quiet=True)
                print(f"✅ JSON transformation completed")
            except Exception as e:
                print(f"⚠️  Warning: JSON transformation failed: {e}")
                print(f"   Original JSON file is still available at: {output_path}")
        
        return result
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error generating JSON: {error_msg}")
        
        # Check for common validation errors and provide helpful messages
        if "RangeParameterSettings" in error_msg and "max" in error_msg:
            print("   ⚠️  Warning: Parameter range validation error (missing max value)")
            print("   This is a known issue with some Tableau workbooks.")
        elif "NoneType" in error_msg and "lower" in error_msg:
            print("   ⚠️  Warning: Missing aggregation type in measure")
            print("   This is a known issue with some Tableau workbooks.")
        
        import traceback
        traceback.print_exc()
        raise


def process_local_twb_file(twb_file: str, output_dir: str = "output") -> dict:
    """
    Process a single local TWB file and generate JSON.
    
    Args:
        twb_file: Path to .twb or .twbx file
        output_dir: Directory to save outputs
        
    Returns:
        dict: Processing result
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    twb_path = Path(twb_file)
    if not twb_path.exists():
        raise FileNotFoundError(f"File not found: {twb_path}")
    
    print(f"\n{'='*60}")
    print(f"Processing local TWB file: {twb_path.name}")
    print(f"{'='*60}\n")
    
    try:
        # Generate JSON for the file
        file_output_dir = output_path / twb_path.stem
        json_result = generate_json_from_twb(str(twb_path), str(file_output_dir))
        json_file = str(file_output_dir / "processed_pipeline_output.json")
        
        print(f"\n{'='*60}")
        print(f"✅ Processing complete!")
        print(f"  JSON file: {json_file}")
        print(f"{'='*60}")
        
        return {
            "status": "success",
            "json_file": json_file
        }
    except Exception as e:
        error_msg = f"Error processing {twb_path.name}: {str(e)}"
        print(f"\n{'='*60}")
        print(f"❌ Processing failed!")
        print(f"  Error: {error_msg}")
        print(f"{'='*60}")
        raise


def process_workbook(server, workbook, batch_folder, batch_index, workbook_index, max_retries=3):
    """
    Process a single workbook: download and extract XML with retry logic.
    
    Args:
        server: TSC Server instance
        workbook: Workbook object
        batch_folder: Folder to save downloaded workbook
        batch_index: Batch number
        workbook_index: Index of workbook in batch
        max_retries: Maximum number of retry attempts for failed downloads (default: 3)
        
    Returns:
        dict: Processing result with status
    """
    safe_name = slugify(workbook.name)
    # Don't include extension - server.workbooks.download() will add .twbx automatically
    file_path = batch_folder / f"{safe_name}_{workbook_index}"
    
    last_error = None
    
    # Retry logic with exponential backoff
    for attempt in range(max_retries):
        try:
            # Download workbook directly
            download_path = server.workbooks.download(workbook.id, filepath=str(file_path))
            
            # Check if download was successful
            if not download_path:
                last_error = "Download returned None"
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff: 1s, 2s, 4s
                    continue
                return {"status": "failed", "name": workbook.name, "error": last_error}
            
            download_path_obj = Path(download_path)
            if not download_path_obj.exists():
                last_error = f"Downloaded file does not exist: {download_path}"
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return {"status": "failed", "name": workbook.name, "error": last_error}
            
            file_size = download_path_obj.stat().st_size
            if file_size == 0:
                last_error = f"Downloaded file is empty (0 bytes): {download_path}"
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)
                    continue
                return {"status": "failed", "name": workbook.name, "error": last_error}
            
            # Validate zip file if it's a .twbx file
            if download_path_obj.suffix.lower() == '.twbx':
                if not validate_zip_file(download_path_obj):
                    last_error = f"Downloaded .twbx file is corrupted or incomplete: {download_path}"
                    # Delete corrupted file before retry
                    try:
                        download_path_obj.unlink()
                    except:
                        pass
                    if attempt < max_retries - 1:
                        time.sleep(2 ** attempt)
                        continue
                    return {"status": "failed", "name": workbook.name, "error": last_error}
            
            # Extract XML from downloaded file
            xml_text = extract_twb_xml(download_path)
            
            if xml_text:
                # If it's a .twbx file, also convert it to .twb and remove .twbx
                if download_path_obj.suffix.lower() == '.twbx':
                    twb_path = convert_twbx_to_twb(download_path_obj, remove_twbx=True)
                    if twb_path:
                        return {
                            "status": "success", 
                            "name": workbook.name, 
                            "file_size": file_size,
                            "twb_path": str(twb_path)
                        }
                
                return {"status": "success", "name": workbook.name, "file_size": file_size}
            else:
                return {"status": "warning", "name": workbook.name, "error": "Could not extract XML"}
                
        except Exception as ex:
            import traceback
            error_details = f"{str(ex)}\n{traceback.format_exc()}"
            last_error = error_details
            
            # Check if it's a connection/incomplete read error
            error_str = str(ex).lower()
            if any(keyword in error_str for keyword in ['incompleteread', 'connection broken', 'response ended', 'prematurely']):
                if attempt < max_retries - 1:
                    # Delete potentially corrupted file
                    try:
                        if 'download_path_obj' in locals() and download_path_obj.exists():
                            download_path_obj.unlink()
                    except:
                        pass
                    wait_time = 2 ** attempt
                    print(f"  ⚠ Retrying download for {workbook.name} (attempt {attempt + 2}/{max_retries}) after {wait_time}s...")
                    time.sleep(wait_time)
                    continue
            
            # If not a retryable error or max retries reached, return error
            if attempt == max_retries - 1:
                return {"status": "error", "name": workbook.name, "error": error_details}
    
    # If we get here, all retries failed
    return {"status": "error", "name": workbook.name, "error": last_error or "All retry attempts failed"}


def download_workbooks_from_server(
    server_url,
    username,
    password,
    site_id,
    json_output_dir="output"
):
    """
    Download all workbooks from Tableau server and generate JSON.
    
    Args:
        server_url: Tableau server URL (required)
        username: Tableau username (required)
        password: Tableau password (required)
        site_id: Tableau site ID (required)
        json_output_dir: Directory to save JSON files (default: output)
        
    Returns:
        dict: Dictionary with download statistics
    """
    # Validate required variables
    if not server_url:
        raise ValueError("--server-url is required")
    if not username:
        raise ValueError("--username is required")
    if not password:
        raise ValueError("--password is required")
    if not site_id:
        raise ValueError("--site-id is required")
    
    # Debug: Print what was loaded (mask password)
    print(f"\nUsing Tableau server credentials:")
    print(f"  SERVER_URL: {server_url}")
    print(f"  USERNAME: {username}")
    print(f"  PASSWORD: ***")
    print(f"  SITE_ID: {site_id}")
    
    batch_size = 10
    temp_dir = Path("temp_tableau_workbooks")
    max_workers = 5
    
    # Set up directories
    temp_dir.mkdir(exist_ok=True)
    
    # Configure server
    server = TSC.Server(server_url, use_server_version=True)
    
    # Authenticate with Tableau server
    tableau_auth = TSC.TableauAuth(username, password, site_id=site_id)
    
    total_workbooks = 0
    batch_folders = []
    failed_workbooks = []
    
    with server.auth.sign_in(tableau_auth):
        # Get all workbooks across the site
        all_workbooks = []
        req_option = TSC.RequestOptions(pagesize=1000)  # max allowed per page
        page, pagination_item = server.workbooks.get(req_option)
        all_workbooks.extend(page)

        # Loop through additional pages if needed
        while pagination_item.page_number < (
            pagination_item.total_available // pagination_item.page_size + 1
        ):
            req_option.page_number += 1
            page, pagination_item = server.workbooks.get(req_option)
            all_workbooks.extend(page)

        filtered_workbooks = all_workbooks
        total_workbooks = len(filtered_workbooks)
        print(f"Found {total_workbooks} workbooks to download")
        print(f"Using {max_workers} concurrent workers for faster downloads\n")

        # Process workbooks in batches
        for batch_index in range(0, len(filtered_workbooks), batch_size):
            batch_workbooks = filtered_workbooks[batch_index:batch_index + batch_size]
            batch_number = (batch_index // batch_size) + 1
            
            batch_folder = temp_dir / f"batch_{batch_number}"
            batch_folder.mkdir(parents=True, exist_ok=True)
            batch_folders.append(batch_folder)

            print(f"Processing batch {batch_number} ({len(batch_workbooks)} workbooks)...")

            # Process workbooks concurrently
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                # Submit all workbooks in the batch
                future_to_workbook = {
                    executor.submit(
                        process_workbook,
                        server, workbook, batch_folder,
                        batch_number, i, 3  # max_retries=3
                    ): workbook
                    for i, workbook in enumerate(batch_workbooks)
                }
                
                # Process completed downloads as they finish
                for future in as_completed(future_to_workbook):
                    workbook = future_to_workbook[future]
                    try:
                        result = future.result()
                        if result["status"] == "success":
                            file_size_info = f" ({result.get('file_size', 0) / 1024:.1f} KB)" if result.get('file_size') else ""
                            print(f"  ✓ {result['name']}{file_size_info}")
                        elif result["status"] == "warning":
                            print(f"  ⚠ {result['name']}: {result.get('error', 'Warning')}")
                        else:
                            error_msg = result.get('error', 'Failed')
                            # Print first line of error for brevity, full error in details
                            error_first_line = error_msg.split('\n')[0] if '\n' in error_msg else error_msg
                            print(f"  ✗ {result['name']}: {error_first_line}")
                            if '\n' in error_msg:
                                print(f"      Full error details logged above")
                            failed_workbooks.append(result["name"])
                    except Exception as ex:
                        import traceback
                        print(f"  ✗ {workbook.name}: Exception - {ex}")
                        print(f"      Traceback: {traceback.format_exc()}")
                        failed_workbooks.append(workbook.name)
    
    print(f"\n{'='*60}")
    print(f"Download complete!")
    print(f"  Total workbooks: {total_workbooks}")
    print(f"  Successfully processed: {total_workbooks - len(failed_workbooks)}")
    print(f"  Failed: {len(failed_workbooks)}")
    print(f"  Workbooks saved to: {temp_dir}")
    if failed_workbooks:
        print(f"\nFailed workbooks: {', '.join(failed_workbooks)}")
    print(f"{'='*60}")
    
    json_files = []
    json_errors = []  # Track files that failed JSON conversion
    
    print(f"\n{'='*60}")
    print(f"Generating JSON from downloaded workbooks...")
    print(f"{'='*60}")
    
    json_output_dir = Path(json_output_dir)
    json_output_dir.mkdir(parents=True, exist_ok=True)
    
    # Find all downloaded files and convert .twbx to .twb automatically
    downloaded_files = []
    for batch_folder in batch_folders:
        # First, convert any remaining .twbx files to .twb and remove .twbx
        twbx_files = list(batch_folder.glob("*.twbx"))
        for twbx_file in twbx_files:
            print(f"Converting {twbx_file.name} to .twb format...")
            twb_path = convert_twbx_to_twb(twbx_file, remove_twbx=True)
            if twb_path:
                print(f"  ✓ Converted and removed .twbx file")
            else:
                print(f"  ⚠ Could not convert {twbx_file.name}, will try to process as-is")
        
        # Now collect all .twb files (including newly converted ones)
        downloaded_files.extend(list(batch_folder.glob("*.twb")))
    
    print(f"Found {len(downloaded_files)} downloaded workbook(s) to process")
    
    for i, twb_file in enumerate(downloaded_files, 1):
        print(f"\n[{i}/{len(downloaded_files)}] Generating JSON from: {twb_file.name}")
        try:
            # Remove double extension if present (e.g., .twb.twb -> .twb)
            file_stem = twb_file.stem
            if file_stem.endswith('.twb'):
                # Already has extension in stem, use it as is
                file_output_dir = json_output_dir / file_stem
            else:
                # Use the stem normally
                file_output_dir = json_output_dir / twb_file.stem
            
            generate_json_from_twb(str(twb_file), str(file_output_dir))
            json_files.append(str(file_output_dir / "processed_pipeline_output.json"))
            print(f"  ✅ Successfully generated and transformed JSON for {twb_file.name}")
        except Exception as e:
            import traceback
            error_traceback = traceback.format_exc()
            error_message = str(e)
            
            # Extract the main error type and message
            error_type = type(e).__name__
            error_summary = error_message.split('\n')[0] if '\n' in error_message else error_message
            
            # Store error information
            json_errors.append({
                "file": twb_file.name,
                "file_path": str(twb_file),
                "error_type": error_type,
                "error_message": error_summary,
                "full_error": error_traceback
            })
            
            print(f"  ❌ FAILED to generate JSON for {twb_file.name}")
            print(f"     Error Type: {error_type}")
            print(f"     Error: {error_summary}")
            # Continue with next file instead of stopping
    
    # Print summary
    print(f"\n{'='*60}")
    print(f"✅ JSON generation complete!")
    print(f"{'='*60}")
    print(f"  Total files processed: {len(downloaded_files)}")
    print(f"  ✅ Successfully converted: {len(json_files)}")
    print(f"  ❌ Failed conversions: {len(json_errors)}")
    print(f"  Output directory: {json_output_dir}")
    
    # Print detailed error information if any failures occurred
    if json_errors:
        print(f"\n{'='*60}")
        print(f"❌ FILES WITH JSON CONVERSION ERRORS:")
        print(f"{'='*60}")
        for idx, error_info in enumerate(json_errors, 1):
            print(f"\n[{idx}] File: {error_info['file']}")
            print(f"    Path: {error_info['file_path']}")
            print(f"    Error Type: {error_info['error_type']}")
            print(f"    Error Message: {error_info['error_message']}")
            print(f"    Full Error Details:")
            # Print first few lines of full error for context
            error_lines = error_info['full_error'].split('\n')
            for line in error_lines[:10]:  # Show first 10 lines
                if line.strip():
                    print(f"      {line}")
            if len(error_lines) > 10:
                print(f"      ... ({len(error_lines) - 10} more lines)")
        print(f"\n{'='*60}")
    
    return {
        "total_workbooks": total_workbooks,
        "batch_folders": batch_folders,
        "temp_dir": temp_dir,
        "failed_workbooks": failed_workbooks,
        "json_files": json_files,
        "json_errors": json_errors
    }


def main():
    """Main entry point for the tableau-assess CLI command."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Process Tableau workbooks and generate JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Process local TWB file
  tableau-assess --local "C:\\Users\\User\\Downloads\\Hierarchy-and-Actions.twb" --generate-json
  
  # Download from server and generate JSON
  tableau-assess --server --server-url https://tableau.squareshift.dev --username admin --password Squareshift123@ --site-id prod --generate-json
  
  # Download from server and generate both JSON files (with PostgreSQL metrics)
  tableau-assess --server --server-url https://tableau.squareshift.dev --username admin --password Squareshift123@ --site-id prod --generate-json --pg-host 34.46.219.118 --pg-port 8060 --pg-db workgroup --pg-user readonly --pg-password Test@123
        """
    )
    
    # Mode selection
    mode_group = parser.add_mutually_exclusive_group(required=True)
    mode_group.add_argument(
        "--server",
        action="store_true",
        help="Download workbooks from Tableau server"
    )
    mode_group.add_argument(
        "--local",
        type=str,
        metavar="FILE",
        help="Process local TWB/TWBX file (single file path)"
    )
    
    # Server-specific options (required when --server is used)
    parser.add_argument(
        "--server-url",
        type=str,
        help="Tableau server URL (required for --server mode)"
    )
    parser.add_argument(
        "--username",
        type=str,
        help="Tableau username (required for --server mode)"
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Tableau password (required for --server mode)"
    )
    parser.add_argument(
        "--site-id",
        type=str,
        help="Tableau site ID (required for --server mode)"
    )
    
    # Optional PostgreSQL arguments for metrics generation
    parser.add_argument(
        "--pg-host",
        type=str,
        help="PostgreSQL host (optional, for metrics generation)"
    )
    parser.add_argument(
        "--pg-port",
        type=int,
        help="PostgreSQL port (optional, for metrics generation)"
    )
    parser.add_argument(
        "--pg-db",
        type=str,
        help="PostgreSQL database name (optional, for metrics generation)"
    )
    parser.add_argument(
        "--pg-user",
        type=str,
        help="PostgreSQL user (optional, for metrics generation)"
    )
    parser.add_argument(
        "--pg-password",
        type=str,
        help="PostgreSQL password (optional, for metrics generation)"
    )
    parser.add_argument(
        "--pg-sslmode",
        type=str,
        help="PostgreSQL SSL mode (optional, for metrics generation)"
    )
    parser.add_argument(
        "--pg-connect-timeout",
        type=int,
        help="PostgreSQL connection timeout in seconds (optional, for metrics generation)"
    )
    
    # JSON generation (required)
    parser.add_argument(
        "--generate-json",
        action="store_true",
        required=True,
        help="Generate JSON output using MigrationEngine (required)"
    )
    
    args = parser.parse_args()
    
    # Validate --generate-json is always provided
    if not args.generate_json:
        parser.error("--generate-json is required")
    
    if args.server:
        # Validate server mode required arguments
        if not args.server_url:
            parser.error("--server-url is required when using --server mode")
        if not args.username:
            parser.error("--username is required when using --server mode")
        if not args.password:
            parser.error("--password is required when using --server mode")
        if not args.site_id:
            parser.error("--site-id is required when using --server mode")
        
        # Download from server mode
        print("="*60)
        print("MODE: Download from Tableau Server")
        print("="*60)
        result = download_workbooks_from_server(
            server_url=args.server_url,
            username=args.username,
            password=args.password,
            site_id=args.site_id,
            json_output_dir="output"
        )
        print(f"\n📊 Final Statistics:")
        print(f"  Workbooks: {result['total_workbooks']}")
        print(f"  JSON files: {len(result.get('json_files', []))}")
        json_errors = result.get('json_errors', [])
        if json_errors:
            print(f"  JSON conversion errors: {len(json_errors)}")
            print(f"\n  Files with JSON errors:")
            for error in json_errors:
                print(f"    - {error['file']}: {error['error_type']} - {error['error_message']}")
        
        # Generate metrics JSON if PostgreSQL arguments are provided
        # Check if any PostgreSQL argument is provided (to determine if metrics should be generated)
        has_pg_args = any([
            args.pg_host,
            args.pg_port,
            args.pg_db,
            args.pg_user,
            args.pg_password,
            args.pg_sslmode,
            args.pg_connect_timeout
        ])
        
        if has_pg_args:
            print("\n" + "="*60)
            print("MODE: Generating Metrics JSON from PostgreSQL")
            print("="*60)
            
            # Validate all required PostgreSQL parameters are provided
            required_pg_params = {
                "pg_host": args.pg_host,
                "pg_port": args.pg_port,
                "pg_db": args.pg_db,
                "pg_user": args.pg_user,
                "pg_password": args.pg_password,
            }
            
            missing_params = [param for param, value in required_pg_params.items() if not value]
            if missing_params:
                print(f"\n❌ Error: Missing required PostgreSQL parameters: {', '.join(missing_params)}")
                print("   All PostgreSQL parameters must be provided when generating metrics.")
                print("   Required: --pg-host, --pg-port, --pg-db, --pg-user, --pg-password")
                print("   Optional: --pg-sslmode (default: 'prefer'), --pg-connect-timeout (default: 15)")
            else:
                try:
                    from tableau_to_looker_parser.metrics import generate_metrics_json
                    
                    # Fetch the Site LUID from Tableau API using the site content URL
                    try:
                        site_luid = fetch_site_luid(
                            server_url=args.server_url,
                            username=args.username,
                            password=args.password,
                            site_content_url=args.site_id
                        )
                    except Exception as e:
                        print(f"\n❌ Error fetching Site LUID: {e}")
                        print("   Skipping metrics generation.")
                        import traceback
                        traceback.print_exc()
                        site_luid = None
                    
                    if site_luid:
                        metrics_file = generate_metrics_json(
                            site_id=site_luid,  # Use the fetched LUID instead of content URL
                            pg_host=args.pg_host,
                            pg_port=args.pg_port,
                            pg_db=args.pg_db,
                            pg_user=args.pg_user,
                            pg_password=args.pg_password,
                            pg_sslmode=args.pg_sslmode or "prefer",
                            pg_connect_timeout=args.pg_connect_timeout or 15,
                            output_dir="output"
                        )
                        print(f"\n✅ Metrics JSON generated successfully: {metrics_file}")
                except ImportError as e:
                    print(f"\n⚠️  Warning: Could not import metrics module: {e}")
                    print("   Make sure psycopg is installed: pip install psycopg[binary]")
                except Exception as e:
                    print(f"\n❌ Error generating metrics JSON: {e}")
                    import traceback
                    traceback.print_exc()
    
    elif args.local:
        # Process local file mode
        print("="*60)
        print("MODE: Process Local TWB File")
        print("="*60)
        result = process_local_twb_file(
            twb_file=args.local,
            output_dir="output"
        )
        print(f"\n📊 Final Statistics:")
        print(f"  Status: {result['status']}")
        if result['status'] == 'success':
            print(f"  JSON file: {result['json_file']}")


if __name__ == "__main__":
    main()

