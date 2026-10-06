import re

def parse_torrent_title(title):
    """
    Parses torrent title strings into structured metadata.
    Extracts clean title, quality, release year, season, and episode.
    """
    if not title:
        return {"clean_title": "Unknown", "year": None, "quality": "Unknown"}
        
    # Extract Year (e.g. 19xx or 20xx)
    year_match = re.search(r'\b(19\d\d|20\d\d)\b', title)
    year = int(year_match.group(1)) if year_match else None
    
    # Extract Quality
    quality_match = re.search(r'\b(720p|1080p|2160p|4k|bluray|webrip)\b', title, re.IGNORECASE)
    quality = quality_match.group(1).upper() if quality_match else "SD"
    
    # Extract Clean Title (everything before year or season info)
    clean_title = re.split(r'\b(19\d\d|20\d\d|S\d\dE\d\d|720p|1080p)\b', title, flags=re.IGNORECASE)[0]
    clean_title = clean_title.replace('.', ' ').replace('_', ' ').strip()
    
    return {
        "clean_title": clean_title if clean_title else title,
        "year": year,
        "quality": quality
    }

if __name__ == "__main__":
    test_title = "Inception.2010.1080p.BluRay.x264"
    print(f"Sample Parse: {parse_torrent_title(test_title)}")