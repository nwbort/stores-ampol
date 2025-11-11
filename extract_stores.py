import json
import sys
import time
import random
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# --- Configuration ---
SITEMAP_FILE = "locations.ampol.com.au-sitemap.xml.xml"
DEFAULT_WORKERS = 8
MAX_RETRIES = 5
INITIAL_DELAY = 2  # seconds
verbose = False

def extract_urls_from_sitemap(filepath):
    """Extract store URLs from the sitemap XML file."""
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        urls = [
            elem.text for elem in root.findall('.//ns:loc', namespace)
            if 'ampol.com.au/en/' in elem.text
        ]
        return urls
    except Exception as e:
        print(f"Error parsing sitemap: {e}", file=sys.stderr)
        return []

def get_store_details(url):
    """Fetch a store page with retry logic and extract details from JSON-LD."""
    headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
    req = Request(url, headers=headers)
    
    for attempt in range(MAX_RETRIES):
        try:
            if attempt > 0 and verbose:
                print(f"Retrying: {url} (Attempt {attempt + 1}/{MAX_RETRIES})", file=sys.stderr)
            
            with urlopen(req, timeout=15) as response:
                html = response.read().decode('utf-8')
            
            # --- Start of parsing logic ---
            start_marker = '<script type="application/ld+json">'
            end_marker = '</script>'
            start_idx = html.find(start_marker)
            
            if start_idx == -1: return None
            start_idx += len(start_marker)
            end_idx = html.find(end_marker, start_idx)
            if end_idx == -1: return None

            json_str = html[start_idx:end_idx]
            raw_data = json.loads(json_str)
            
            if isinstance(raw_data, dict) and '@graph' in raw_data:
                data_list = raw_data['@graph']
            elif isinstance(raw_data, list):
                data_list = raw_data
            else:
                data_list = [raw_data]

            store_info, services = None, []
            for item in data_list:
                if not isinstance(item, dict): continue
                item_type = item.get('@type')
                if item_type == 'LocalBusiness': store_info = item
                elif item_type == 'Service' and 'serviceType' in item: services.append(item['serviceType'])
            
            if not store_info: return None

            address_info = store_info.get('address', {})
            street = address_info.get('streetAddress') if isinstance(address_info, dict) else None
            locality = address_info.get('addressLocality') if isinstance(address_info, dict) else None
            postcode = address_info.get('postalCode') if isinstance(address_info, dict) else None
            country_obj = address_info.get('addressCountry') if isinstance(address_info, dict) else {}
            country = country_obj.get('name') if isinstance(country_obj, dict) else None
            
            geo_info = store_info.get('geo', {})
            latitude = float(geo_info['latitude']) if isinstance(geo_info, dict) and geo_info.get('latitude') else None
            longitude = float(geo_info['longitude']) if isinstance(geo_info, dict) and geo_info.get('longitude') else None
            
            opening_hours = []
            hours_specs = store_info.get('openingHoursSpecification', [])
            if not isinstance(hours_specs, list): hours_specs = [hours_specs]
            for spec in hours_specs:
                if isinstance(spec, dict):
                    day = spec.get('dayOfWeek', '').split('/')[-1]
                    opening_hours.append({'dayOfWeek': day, 'opens': spec.get('opens'), 'closes': spec.get('closes')})

            return {
                'ref': store_info.get('@id'), 'name': store_info.get('name'), 'url': store_info.get('url'),
                'phone': store_info.get('telephone'), 'address': street, 'locality': locality, 'postcode': postcode,
                'country': country, 'latitude': latitude, 'longitude': longitude, 'openingHours': opening_hours,
                'services': sorted(list(set(services)))
            }
            # --- End of parsing logic ---

        except HTTPError as e:
            if e.code == 429 and attempt < MAX_RETRIES - 1:
                # Exponential backoff with jitter
                delay = (INITIAL_DELAY * 2**attempt) + random.uniform(0, 1)
                if verbose:
                    print(f"Rate limited for {url}. Retrying in {delay:.2f}s...", file=sys.stderr)
                time.sleep(delay)
                continue # Go to next attempt in the loop
            else:
                print(f"Error fetching {url}: {e}", file=sys.stderr)
                return None # Non-429 error, or final retry failed
        except (URLError, json.JSONDecodeError) as e:
            print(f"Error processing {url}: {e}", file=sys.stderr)
            return None
        except Exception as e:
            print(f"An unexpected error occurred for {url}: {e}", file=sys.stderr)
            return None
            
    return None # If all retries fail

def sort_opening_hours(store_data):
    """Sort opening hours from Monday to Sunday."""
    if store_data and 'openingHours' in store_data and store_data['openingHours']:
        day_order = {'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3, 'Friday': 4, 'Saturday': 5, 'Sunday': 6}
        store_data['openingHours'] = sorted(store_data['openingHours'], key=lambda x: day_order.get(x.get('dayOfWeek', ''), 7))
    return store_data

def main():
    """Main function to scrape all stores and output as JSON."""
    global verbose
    
    parser = argparse.ArgumentParser(description='Extract Ampol store details from sitemap.')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose output')
    parser.add_argument('-w', '--workers', type=int, default=DEFAULT_WORKERS, help=f'Number of parallel workers (default: {DEFAULT_WORKERS})')
    args = parser.parse_args()
    verbose = args.verbose
    
    if not Path(SITEMAP_FILE).exists():
        print(f"Error: Sitemap file '{SITEMAP_FILE}' not found.", file=sys.stderr)
        sys.exit(1)
    
    urls = extract_urls_from_sitemap(SITEMAP_FILE)
    if not urls:
        print("Error: No store URLs found in sitemap.", file=sys.stderr)
        sys.exit(1)

    if verbose:
        print(f"Found {len(urls)} stores in sitemap", file=sys.stderr)
    
    all_stores, errors = [], []
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_url = {executor.submit(get_store_details, url): (i, url) for i, url in enumerate(urls, 1)}
        for future in as_completed(future_to_url):
            i, url = future_to_url[future]
            try:
                store_data = future.result()
                if store_data:
                    store_data = sort_opening_hours(store_data)
                    all_stores.append(store_data)
                    if verbose:
                        print(f"  [{i}/{len(urls)}] OK: {store_data.get('name', 'Unknown')}", file=sys.stderr)
                else:
                    errors.append((i, url))
                    if verbose:
                        print(f"  [{i}/{len(urls)}] FAILED to extract: {url}", file=sys.stderr)
            except Exception as e:
                errors.append((i, url))
                print(f"Error processing future for {url}: {e}", file=sys.stderr)
    
    print(f"\nExtracted {len(all_stores)} stores", file=sys.stderr)
    if errors:
        print(f"Failed to extract {len(errors)} stores:", file=sys.stderr)
        for idx, url in errors:
            print(f"  [{idx}] {url}", file=sys.stderr)
    
    all_stores_sorted = sorted(all_stores, key=lambda x: x.get('ref', ''))
    print(json.dumps(all_stores_sorted, indent=2))

if __name__ == "__main__":
    main()
