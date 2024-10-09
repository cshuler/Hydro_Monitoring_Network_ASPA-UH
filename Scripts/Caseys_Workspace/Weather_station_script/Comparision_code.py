def extract_date_from_filename(filename):
    # Define common date patterns (dd.mm.yyyy, mm.dd.yyyy, etc.)
    date_patterns = [
        r'(\d{1,2})\.(\d{1,2})\.(\d{4})',  # dd.mm.yyyy or d.m.yyyy
        r'(\d{4})-(\d{2})-(\d{2})',  # yyyy-mm-dd
        r'(\d{1,2})-(\d{1,2})-(\d{4})',  # mm-dd-yyyy or m-d-yyyy
    ]
    
    for pattern in date_patterns:
        match = re.search(pattern, filename)
        if match:
            try:
                # Handle different date formats
                if len(match.groups()) == 3:  # Full date pattern
                    if '.' in match.group(0):  # dd.mm.yyyy or mm.dd.yyyy
                        day, month, year = match.groups()
                        return datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')
                    elif '-' in match.group(0):  # yyyy-mm-dd or mm-dd-yyyy
                        year, month, day = match.groups()
                        return datetime.strptime(f'{year}-{month}-{day}', '%Y-%m-%d')
            except ValueError as e:
                logging.error(f"Error parsing date from filename {filename}: {e}")
                return None

    logging.warning(f"No valid date found in filename: {filename}")
    return None
