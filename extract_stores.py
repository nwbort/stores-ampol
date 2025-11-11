import json
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from urllib.request import urlopen, Request
from urllib.error import URLError, HTTPError
from concurrent.futures import ThreadPoolExecutor, as_completed
import argparse

# The filename is constructed from the URL by download.sh
SITEMAP_FILE = "locations.ampol.com.au-sitemap.xml.xml"
DEFAULT_WORKERS = 8
verbose = False

def extract_urls_from_sitemap(filepath):
    """Extract store URLs from the sitemap XML file."""
    try:
        tree = ET.parse(filepath)
        root = tree.getroot()
        namespace = {'ns': 'http://www.sitemaps.org/schemas/sitemap/0.9'}
        # Filter to get only the individual store location pages
        urls = [
            elem.text for elem in root.findall('.//ns:loc', namespace)
            if 'ampol.com.au/en/' in elem.text
        ]
        return urls
    except Exception as e:
        print(f"Error parsing sitemap: {e}", file=sys.stderr)
        return []

def get_store_details(url):
    """Fetch a store page and extract details from the JSON-LD data."""
    try:
        if verbose:
            print(f"Fetching: {url}", file=sys.stderr)

        headers = {'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'}
        req = Request(url, headers=headers)
        
        with urlopen(req, timeout=10) as response:
            html = response.read().decode('utf-8')
        
        start_marker = '<script type="application/ld+json">'
        end_marker = '</script>'
        start_idx = html.find(start_marker)
        
        if start_idx == -1:
            return None

        start_idx += len(start_marker)
        end_idx = html.find(end_marker, start_idx)
        
        if end_idx == -1:
            return None

        json_str = html[start_idx:end_idx]
        raw_data = json.loads(json_str)
        
        # JSON-LD can be a single object, a list of objects, or a graph object
        if isinstance(raw_data, dict) and '@graph' in raw_data:
            data_list = raw_data['@graph']
        elif isinstance(raw_data, list):
            data_list = raw_data
        else:
            data_list = [raw_data]

        store_info = None
        services = []
        for item in data_list:
            if not isinstance(item, dict):
                continue
            item_type = item.get('@type')
            if item_type == 'LocalBusiness':
                store_info = item
            elif item_type == 'Service' and 'serviceType' in item:
                services.append(item['serviceType'])
        
        if not store_info:
            return None

        # Defensively get nested properties
        address_info = store_info.get('address')
        street = locality = postcode = country = None
        if isinstance(address_info, dict):
            street = address_info.get('streetAddress')
            locality = address_info.get('addressLocality')
            postcode = address_info.get('postalCode')
            country_obj = address_info.get('addressCountry')
            if isinstance(country_obj, dict):
                country = country_obj.get('name')

        geo_info = store_info.get('geo')
        latitude = longitude = None
        if isinstance(geo_info, dict):
            lat_str = geo_info.get('latitude')
            lon_str = geo_info.get('longitude')
            try:
                if lat_str is not None: latitude = float(lat_str)
                if lon_str is not None: longitude = float(lon_str)
            except (ValueError, TypeError):
                pass # Keep as None if conversion fails

        opening_hours = []
        hours_specs = store_info.get('openingHoursSpecification', [])
        if not isinstance(hours_specs, list):
            hours_specs = [hours_specs]
            
        for spec in hours_specs:
            if isinstance(spec, dict):
                day = spec.get('dayOfWeek', '').split('/')[-1]
                opening_hours.append({
                    'dayOfWeek': day,
                    'opens': spec.get('opens'),
                    'closes': spec.get('closes')
                })

        store_data = {
            'ref': store_info.get('@id'),
            'name': store_info.get('name'),
            'url': store_info.get('url'),
            'phone': store_info.get('telephone'),
            'address': street,
            'locality': locality,
            'postcode': postcode,
            'country': country,
            'latitude': latitude,
            'longitude': longitude,
            'openingHours': opening_hours,
            'services': sorted(list(set(services)))
        }
        
        return store_data
        
    except (URLError, HTTPError) as e:
        print(f"Error fetching {url}: {e}", file=sys.stderr)
        return None
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON from {url}: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error processing {url}: {e}", file=sys.stderr)
        return None

def sort_opening_hours(store_data):
    """Sort opening hours from Monday to Sunday."""
    if store_data and 'openingHours' in store_data and store_data['openingHours']:
        day_order = {
            'Monday': 0, 'Tuesday': 1, 'Wednesday': 2, 'Thursday': 3,
            'Friday': 4, 'Saturday': 5, 'Sunday': 6
        }
        store_data['openingHours'] = sorted(
            store_data['openingHours'],
            key=lambda x: day_order.get(x.get('dayOfWeek', ''), 7)
        )
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
    
    all_stores = []
    errors = []
    
    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        future_to_url = {
            executor.submit(get_store_details, url): (i, url) 
            for i, url in enumerate(urls, 1)
        }
        
        for future in as_completed(future_to_url):
            i, url = future_to_url[future]
            try:
                store_data = future.result()
                if store_data:
                    store_data = sort_opening_hours(store_data)
                    all_stores.append(store_data)
                    if verbose:
                        print(f"  [{i}/{len(urls)}] {store_data.get('name', 'Unknown')}", file=sys.stderr)
                else:
                    errors.append((i, url))
                    if verbose:
                        print(f"  [{i}/{len(urls)}] Failed to extract", file=sys.stderr)
            except Exception as e:
                errors.append((i, url))
                print(f"Error processing {url}: {e}", file=sys.stderr)
    
    print(f"Extracted {len(all_stores)} stores", file=sys.stderr)
    if errors:
        print(f"Failed to extract {len(errors)} stores:", file=sys.stderr)
        for idx, url in errors:
            print(f"  [{idx}] {url}", file=sys.stderr)
    
    all_stores_sorted = sorted(all_stores, key=lambda x: x.get('ref', ''))
    print(json.dumps(all_stores_sorted, indent=2))

if __name__ == "__main__":
    main()
