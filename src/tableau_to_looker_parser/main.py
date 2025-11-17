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
from pathlib import Path
from dotenv import dotenv_values
import tableauserverclient as TSC
from slugify import slugify
from concurrent.futures import ThreadPoolExecutor, as_completed

from tableau_to_looker_parser.core.migration_engine import MigrationEngine
from tableau_to_looker_parser.converter import transform_json


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


def convert_twbx_files_in_directory(directory_path, recursive=False, remove_twbx=True):
    """
    Convert all .twbx files in a directory to .twb files.
    
    Args:
        directory_path: Path to directory containing .twbx files
        recursive: If True, search subdirectories recursively (default: False)
        remove_twbx: If True, delete .twbx files after successful conversion (default: True)
        
    Returns:
        dict: Conversion statistics
    """
    directory = Path(directory_path)
    if not directory.exists():
        print(f"Error: Directory not found: {directory_path}")
        return {"total": 0, "converted": 0, "failed": 0, "errors": []}
    
    # Find all .twbx files
    if recursive:
        twbx_files = list(directory.rglob("*.twbx"))
    else:
        twbx_files = list(directory.glob("*.twbx"))
    
    results = {
        "total": len(twbx_files),
        "converted": 0,
        "failed": 0,
        "errors": []
    }
    
    print(f"\n{'='*60}")
    print(f"Converting {len(twbx_files)} .twbx file(s) to .twb")
    if remove_twbx:
        print(f"(.twbx files will be removed after conversion)")
    print(f"{'='*60}\n")
    
    for i, twbx_file in enumerate(twbx_files, 1):
        print(f"[{i}/{len(twbx_files)}] Converting: {twbx_file.name}")
        twb_path = convert_twbx_to_twb(twbx_file, remove_twbx=remove_twbx)
        if twb_path:
            results["converted"] += 1
        else:
            results["failed"] += 1
            results["errors"].append(f"Failed to convert: {twbx_file.name}")
    
    print(f"\n{'='*60}")
    print(f"Conversion complete!")
    print(f"  Total: {results['total']}")
    print(f"  Converted: {results['converted']}")
    print(f"  Failed: {results['failed']}")
    if results["errors"]:
        print(f"\nErrors:")
        for error in results["errors"]:
            print(f"  - {error}")
    print(f"{'='*60}")
    
    return results


def process_local_twb_files(twb_files: list, output_dir: str = "output", generate_json: bool = False) -> dict:
    """
    Process multiple local TWB files and optionally generate JSON.
    
    Args:
        twb_files: List of paths to .twb or .twbx files
        output_dir: Directory to save outputs
        generate_json: Whether to generate JSON using MigrationEngine
        
    Returns:
        dict: Processing statistics
    """
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    
    results = {
        "total_files": len(twb_files),
        "processed": 0,
        "failed": 0,
        "json_files": [],
        "errors": []
    }
    
    print(f"\n{'='*60}")
    print(f"Processing {len(twb_files)} local TWB file(s)")
    print(f"{'='*60}\n")
    
    for i, twb_file in enumerate(twb_files, 1):
        twb_path = Path(twb_file)
        print(f"\n[{i}/{len(twb_files)}] Processing: {twb_path.name}")
        
        try:
            if generate_json:
                # Generate JSON for each file
                file_output_dir = output_path / twb_path.stem
                json_result = generate_json_from_twb(str(twb_path), str(file_output_dir))
                results["json_files"].append(str(file_output_dir / "processed_pipeline_output.json"))
                results["processed"] += 1
                print(f"✅ Successfully processed: {twb_path.name}")
            else:
                # Just validate the file exists
                if twb_path.exists():
                    results["processed"] += 1
                    print(f"✅ File validated: {twb_path.name}")
                else:
                    raise FileNotFoundError(f"File not found: {twb_path}")
                    
        except Exception as e:
            results["failed"] += 1
            error_msg = f"Error processing {twb_path.name}: {str(e)}"
            results["errors"].append(error_msg)
            print(f"❌ {error_msg}")
    
    print(f"\n{'='*60}")
    print(f"Processing complete!")
    print(f"  Total: {results['total_files']}")
    print(f"  Processed: {results['processed']}")
    print(f"  Failed: {results['failed']}")
    if results["json_files"]:
        print(f"  JSON files generated: {len(results['json_files'])}")
    if results["errors"]:
        print(f"\nErrors:")
        for error in results["errors"]:
            print(f"  - {error}")
    print(f"{'='*60}")
    
    return results


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
    server_url=None,
    username=None,
    password=None,
    site_id=None,
    batch_size=10,
    temp_dir=None,
    project_name=None,
    max_workers=5,
    generate_json=False,
    json_output_dir=None,
    env_file=None
):
    """
    Download all workbooks from Tableau server.
    
    Args:
        server_url: Tableau server URL (required if not in env_file)
        username: Tableau username (required if not in env_file)
        password: Tableau password (required if not in env_file)
        site_id: Tableau site ID (optional)
        batch_size: Number of workbooks to process in each batch (default: 10)
        temp_dir: Directory to store downloaded workbooks (default: "temp_tableau_workbooks")
        project_name: Optional project name filter (None = all workbooks)
        max_workers: Number of concurrent downloads (default: 5)
        generate_json: Whether to generate JSON from downloaded workbooks (default: False)
        json_output_dir: Directory to save JSON files (default: output)
        env_file: Optional path to .env file as fallback if arguments not provided
        
    Returns:
        dict: Dictionary with download statistics
    """
    # Load environment variables from arguments or .env file
    TABLEAU_SERVER_URL = server_url
    USERNAME = username
    PASSWORD = password
    SITE_ID = site_id
    
    # If arguments not provided, try to load from .env file
    if not all([TABLEAU_SERVER_URL, USERNAME, PASSWORD]):
        if env_file:
            env_path = Path(env_file)
        else:
            env_path = Path(".env")
        
        if env_path.exists():
            if dotenv_values is None:
                print(f"Warning: python-dotenv not installed. Cannot load .env file. Please install it with: pip install python-dotenv")
            else:
                print(f"Loading environment variables from: {env_path.absolute()}")
                env_vars = dotenv_values(env_path)
                
                TABLEAU_SERVER_URL = TABLEAU_SERVER_URL or env_vars.get("TABLEAU_SERVER_URL")
                USERNAME = USERNAME or env_vars.get("USERNAME") or env_vars.get("TABLEAU_USERNAME")
                PASSWORD = PASSWORD or env_vars.get("PASSWORD") or env_vars.get("TABLEAU_PASSWORD")
                SITE_ID = SITE_ID or env_vars.get("SITE_ID") or env_vars.get("TABLEAU_SITE_ID")
        else:
            if env_file:
                print(f"Warning: Specified .env file not found: {env_path}")
    
    # Debug: Print what was loaded (mask password)
    print(f"\nUsing Tableau server credentials:")
    print(f"  TABLEAU_SERVER_URL: {TABLEAU_SERVER_URL}")
    print(f"  USERNAME: {USERNAME}")
    print(f"  PASSWORD: {'***' if PASSWORD else 'NOT SET'}")
    print(f"  SITE_ID: {SITE_ID}")
    
    # Validate required variables
    if not TABLEAU_SERVER_URL:
        raise ValueError("TABLEAU_SERVER_URL is required. Provide as argument or set in .env file")
    if not USERNAME:
        raise ValueError("USERNAME is required. Provide as argument or set in .env file")
    if not PASSWORD:
        raise ValueError("PASSWORD is required. Provide as argument or set in .env file")
    
    # Set up directories
    if temp_dir is None:
        temp_dir = Path("temp_tableau_workbooks")
    else:
        temp_dir = Path(temp_dir)
    
    temp_dir.mkdir(exist_ok=True)
    
    # Configure server
    server = TSC.Server(TABLEAU_SERVER_URL, use_server_version=True)
    
    # Authenticate with Tableau server
    tableau_auth = TSC.TableauAuth(USERNAME, PASSWORD, site_id=SITE_ID)
    
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

        # Filter by project if specified
        if project_name:
            filtered_workbooks = [wb for wb in all_workbooks if wb.project_name == project_name]
        else:
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
    if generate_json:
        # Only generate JSON if site_id is provided (ensures it's from a specific site, not default)
        if not SITE_ID:
            print(f"\n{'='*60}")
            print(f"⚠️  WARNING: JSON generation skipped!")
            print(f"{'='*60}")
            print(f"  JSON generation is only allowed when downloading from a specific site.")
            print(f"  Please provide --site-id or set SITE_ID in .env file to generate JSON.")
            print(f"  Workbooks were downloaded but JSON was not generated.")
            print(f"{'='*60}\n")
        else:
            print(f"\n{'='*60}")
            print(f"Generating JSON from downloaded workbooks...")
            print(f"{'='*60}")
            
            if json_output_dir is None:
                json_output_dir = Path("output")
            else:
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
        "json_files": json_files if generate_json else [],
        "json_errors": json_errors if generate_json else []
    }


def main():
    """Main entry point for the tableau-assess CLI command."""
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Download Tableau workbooks from server OR process local TWB files and generate JSON",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Download from server with credentials as arguments
  tableau-assess --server --server-url https://tableau.example.com --username user --password pass --generate-json
  
  # Download from server using .env file (fallback)
  tableau-assess --server --generate-json
  
  # Download from server with custom .env file
  tableau-assess --server --env-file /path/to/.env --generate-json
  
  # Process local TWB files
  tableau-assess --local files/workbook1.twb files/workbook2.twb
  
  # Process local TWB files with custom output directory
  tableau-assess --local files/*.twb --output-dir my_output
  
  # Convert all .twbx files in a directory to .twb
  tableau-assess --convert-twbx temp_tableau_workbooks/batch_1
  
  # Convert .twbx files recursively in all subdirectories
  tableau-assess --convert-twbx temp_tableau_workbooks --recursive
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
        nargs="+",
        metavar="FILE",
        help="Process local TWB/TWBX files (provide one or more file paths)"
    )
    mode_group.add_argument(
        "--convert-twbx",
        type=str,
        metavar="DIR",
        help="Convert all .twbx files in a directory to .twb files"
    )
    
    # Server-specific options
    parser.add_argument(
        "--server-url",
        type=str,
        help="Tableau server URL (required for --server mode, or set in .env file)"
    )
    parser.add_argument(
        "--username",
        type=str,
        help="Tableau username (required for --server mode, or set in .env file)"
    )
    parser.add_argument(
        "--password",
        type=str,
        help="Tableau password (required for --server mode, or set in .env file)"
    )
    parser.add_argument(
        "--site-id",
        type=str,
        help="Tableau site ID (optional, or set in .env file)"
    )
    parser.add_argument(
        "--env-file",
        type=str,
        help="Path to .env file (used as fallback if credentials not provided as arguments)"
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=10,
        help="Number of workbooks per batch when downloading from server (default: 10)"
    )
    parser.add_argument(
        "--max-workers",
        type=int,
        default=5,
        help="Number of concurrent downloads (default: 5)"
    )
    parser.add_argument(
        "--project-name",
        type=str,
        help="Filter workbooks by project name (server mode only)"
    )
    
    # JSON generation options
    parser.add_argument(
        "--generate-json",
        action="store_true",
        help="Generate JSON output using MigrationEngine"
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="output",
        help="Directory to save JSON output (default: output)"
    )
    parser.add_argument(
        "--recursive",
        action="store_true",
        help="Search subdirectories recursively (for --convert-twbx mode)"
    )
    parser.add_argument(
        "--keep-twbx",
        action="store_true",
        help="Keep .twbx files after conversion (default: remove .twbx files after conversion)"
    )
    
    args = parser.parse_args()
    
    if args.server:
        # Download from server mode
        print("="*60)
        print("MODE: Download from Tableau Server")
        print("="*60)
        result = download_workbooks_from_server(
            server_url=args.server_url,
            username=args.username,
            password=args.password,
            site_id=args.site_id,
            batch_size=args.batch_size,
            max_workers=args.max_workers,
            project_name=args.project_name,
            generate_json=args.generate_json,
            json_output_dir=args.output_dir,
            env_file=args.env_file
        )
        print(f"\n📊 Final Statistics:")
        print(f"  Workbooks: {result['total_workbooks']}")
        if args.generate_json:
            print(f"  JSON files: {len(result.get('json_files', []))}")
            json_errors = result.get('json_errors', [])
            if json_errors:
                print(f"  JSON conversion errors: {len(json_errors)}")
                print(f"\n  Files with JSON errors:")
                for error in json_errors:
                    print(f"    - {error['file']}: {error['error_type']} - {error['error_message']}")
    
    elif args.local:
        # Process local files mode
        print("="*60)
        print("MODE: Process Local TWB Files")
        print("="*60)
        result = process_local_twb_files(
            twb_files=args.local,
            output_dir=args.output_dir,
            generate_json=args.generate_json
        )
        print(f"\n📊 Final Statistics:")
        print(f"  Processed: {result['processed']}")
        print(f"  Failed: {result['failed']}")
        if args.generate_json:
            print(f"  JSON files: {len(result.get('json_files', []))}")
    
    elif args.convert_twbx:
        # Convert .twbx files mode
        print("="*60)
        print("MODE: Convert .twbx Files to .twb")
        print("="*60)
        result = convert_twbx_files_in_directory(
            directory_path=args.convert_twbx,
            recursive=args.recursive,
            remove_twbx=not args.keep_twbx  # Remove .twbx unless --keep-twbx is specified
        )
        print(f"\n📊 Final Statistics:")
        print(f"  Total: {result['total']}")
        print(f"  Converted: {result['converted']}")
        print(f"  Failed: {result['failed']}")


if __name__ == "__main__":
    main()

