"""
Local Reddit Image Scraper
Collects new images from dalle2, midjourney, and aiArt subreddits
Saves to local folders with same naming convention
Runs without Airflow - just execute directly
"""

import praw
from datetime import datetime
from pathlib import Path
import json
import requests
import os
import logging

####################################################
# CONFIGURATION
####################################################

DALLE_SUBR = "dalle2"
MIDJ_SUBR = "midjourney"
AIART_SUBR = "aiArt"
SUBREDDITS = [DALLE_SUBR, MIDJ_SUBR, AIART_SUBR]

# Local data directory
DATA_DIR = Path("data_collection/data")

# Reddit API Configuration
REDDIT_CLIENT_ID = "JT_iiB-NFwFyoknGwv5fYA"
REDDIT_CLIENT_SECRET = "aVY6bitDc5BS8j4LX1cWusWjlgCaVQ"
REDDIT_USER_AGENT = "mac:eecs6893Project:v1.0 (by u/Superb-Cap8523)"

# Flairs from your extract_data.ipynb
# flair for each subreddit we are scrapping
FLAIR_DICT = {MIDJ_SUBR: set(['AI Showcase - Midjourney']),
              DALLE_SUBR: set(['DALL·E 2', 'DALL·E 3']),
              AIART_SUBR: set(['Image -  I used x/ai\'s "Imagine Image and Video Generator."  ',
                            'Image - Bing Image Creator :a2:',
                            'Image - BudgetPixel',
                            'Image - BudgetPixel AI',
                            'Image - ChatGPT :a2:',
                            'Image - CivitAI\xa0:a2:',
                            'Image - ComfyUI',
                            'Image - Custom',
                            'Image - DALL E 3 :a2:',
                            'Image - DeepAI',
                            'Image - Eggie.ai',
                            'Image - Etana',
                            'Image - FLUX :a2:',
                            'Image - Gemini',
                            'Image - Google Gemini :a2:',
                            'Image - Google Imagen',
                            'Image - Grok',
                            'Image - Illustrious',
                            'Image - ImageFX.',
                            'Image - Komiko',
                            'Image - Leonardo.ai :a2:',
                            'Image - Mage.space',
                            'Image - Meta AI  :a2:',
                            'Image - Microsoft Copilot',
                            'Image - Midjourney :a2:',
                            'Image - MuleRun Agent',
                            'Image - MuleRun Halloween Costume Agent',
                            'Image - Nightcafe :a2:',
                            'Image - Other: Aierone',
                            'Image - Other: ComfyUI',
                            'Image - Other: Flat AI',
                            'Image - Other: Grok Imagine',
                            'Image - Other: Hailou Ai',
                            'Image - Other: Moescape [Illustrious]',
                            'Image - Other: MuleRunAI',
                            'Image - Other: NovelAI',
                            'Image - Other: PixNova AI',
                            'Image - Other: Please edit, or your post may be deleted.',
                            'Image - Other: Seedream 4.0',
                            'Image - Other: Wan/Grok/Meta/Nano Banana/Qwen',
                            'Image - Other: Whisk',
                            'Image - Qwen',
                            'Image - Seedream',
                            'Image - Seedream 1.0',
                            'Image - Sogni AI',
                            'Image - SoraAI :a2:',
                            'Image - Stable Diffusion + Manual Editing',
                            'Image - Stable Diffusion :a2:',
                            'Image - String AI',
                            'Image - VQGAN+clip',
                            'Image - Wan',
                            'Image - Whisk',
                            'Image - etana'])
            }


# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

####################################################
# HELPER FUNCTIONS
####################################################

def get_existing_image_ids_local(subr):
    """
    Get all existing image IDs from local folder
    
    Args:
        subr: Subreddit name
    
    Returns:
        Set of submission IDs already downloaded
    """
    subr_dir = DATA_DIR / subr
    
    if not subr_dir.exists():
        logger.warning(f"Directory {subr_dir} does not exist yet")
        return set()
    
    existing_ids = set()
    
    # Get all image files
    for img_file in list(subr_dir.glob("img_*.jpg")) + list(subr_dir.glob("img_*.png")):
        # Extract submission ID from filename
        # Format: img_{subr}_{submission_id}.{ext}
        parts = img_file.stem.split("_")
        
        if len(parts) >= 3:
            submission_id = "_".join(parts[2:])
            existing_ids.add(submission_id)
    
    logger.info(f"Found {len(existing_ids)} existing images for r/{subr}")
    return existing_ids

def get_file_extension(url):
    """Extract file extension from URL"""
    from urllib.parse import urlparse
    
    parsed_url = urlparse(url)
    _, ext = os.path.splitext(parsed_url.path)
    
    return ext.lower() if ext else '.jpg'

def collect_new_images_from_subreddit_local(subr, flair):
    """
    Collect new images from a subreddit locally
    
    Args:
        subr: Subreddit name
        flair: Set of valid flairs
    
    Returns:
        Dict with collection statistics
    """
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Collecting new images from r/{subr}")
    logger.info(f"{'='*70}")
    
    # Initialize Reddit client
    reddit = praw.Reddit(
        client_id=REDDIT_CLIENT_ID,
        client_secret=REDDIT_CLIENT_SECRET,
        user_agent=REDDIT_USER_AGENT
    )
    
    # Get existing image IDs
    existing_ids = get_existing_image_ids_local(subr)
    
    # Create directory if it doesn't exist
    subr_dir = DATA_DIR / subr
    subr_dir.mkdir(parents=True, exist_ok=True)
    
    # Collect new posts
    subreddit = reddit.subreddit(subr)
    new_images = []
    stats = {
        'subreddit': subr,
        'checked': 0,
        'passed_flair': 0,
        'passed_image': 0,
        'passed_nsfw': 0,
        'new_images': 0,
        'download_errors': 0,
        'images': []
    }
    
    image_extensions = ('.jpg', '.jpeg', '.png')
    
    # Check most recent 500 posts
    for submission in subreddit.new(limit=500):
        stats['checked'] += 1
        
        # Filter 1: Check flair
        if submission.link_flair_text not in flair:
            continue
        stats['passed_flair'] += 1
        
        # Filter 2: Check if URL is image
        if not submission.url.endswith(image_extensions):
            continue
        stats['passed_image'] += 1
        
        # Filter 3: Skip NSFW
        if submission.over_18:
            continue
        stats['passed_nsfw'] += 1
        
        # Filter 4: Check if NEW (not already downloaded)
        if submission.id in existing_ids:
            continue
        
        # This is a NEW image! Download it
        try:
            response = requests.get(submission.url, timeout=10)
            
            # Get original extension
            original_ext = get_file_extension(submission.url)
            filename = f"img_{subr}_{submission.id}{original_ext}"
            
            # Save to local folder
            filepath = subr_dir / filename
            with open(filepath, 'wb') as f:
                f.write(response.content)
            
            stats['new_images'] += 1
            new_images.append({
                'submission_id': submission.id,
                'flair': submission.link_flair_text,
                'created_at': datetime.fromtimestamp(submission.created_utc).isoformat(),
            })
            logger.info(f"✓ Downloaded new image: {filename}")
        
        except Exception as e:
            stats['download_errors'] += 1
            logger.error(f"✗ Error downloading {submission.url}: {e}")
    
    stats['images'] = new_images
    
    # Log summary
    logger.info(f"\nProcessing summary for r/{subr}:")
    logger.info(f"  Total posts checked: {stats['checked']}")
    logger.info(f"  Passed flair filter: {stats['passed_flair']}")
    logger.info(f"  Passed image filter: {stats['passed_image']}")
    logger.info(f"  Passed NSFW filter: {stats['passed_nsfw']}")
    logger.info(f"  🆕 New images: {stats['new_images']}")
    logger.info(f"  ❌ Download errors: {stats['download_errors']}")
    
    return stats

def update_metadata_json_local(subr, new_images):
    """
    Update metadata.json for dalle2 and aiArt subreddits locally
    
    Args:
        subr: Subreddit name
        new_images: List of new image metadata dicts
    """
    
    if subr not in [DALLE_SUBR, AIART_SUBR]:
        logger.info(f"Skipping metadata update for r/{subr}")
        return
    
    if not new_images:
        logger.info(f"No new images for r/{subr}, skipping metadata update")
        return
    
    try:
        subr_dir = DATA_DIR / subr
        metadata_path = subr_dir / "metadata.json"
        
        # Load existing metadata or create new dict
        if metadata_path.exists():
            with open(metadata_path, 'r') as f:
                metadata = json.load(f)
            logger.info(f"Loaded existing metadata for r/{subr}")
        else:
            metadata = {}
            logger.info(f"Creating new metadata for r/{subr}")
        
        # Add new images to metadata
        for img in new_images:
            # Use local file path as key (same as your original structure)
            img_local_path = str(subr_dir / img['filename'])
            metadata[img_local_path] = {
                'flair': img['flair'],
                'created_at': img['created_at'],
            }
        
        # Save updated metadata
        with open(metadata_path, 'w') as f:
            json.dump(metadata, f, indent=4)
        
        logger.info(f"✓ Updated metadata.json for r/{subr} with {len(new_images)} new entries")
    
    except Exception as e:
        logger.error(f"Error updating metadata for r/{subr}: {e}")

def collect_all_subreddit_images_local():
    """
    Main function: Collect images from all subreddits locally
    Run this script directly without Airflow
    """
    
    logger.info("\n" + "="*70)
    logger.info(f"LOCAL IMAGE COLLECTION STARTED - {datetime.now()}")
    logger.info("="*70)
    
    all_stats = []
    
    # Collect from each subreddit
    for subr in SUBREDDITS:
        stats = collect_new_images_from_subreddit_local(subr, FLAIR_DICT[subr])
        all_stats.append(stats)
        
        # Update metadata for dalle2 and aiArt
        if subr in [DALLE_SUBR, AIART_SUBR]:
            update_metadata_json_local(subr, stats['images'])
    
    # Create daily log
    log_data = {
        'date': datetime.now().isoformat(),
        'subreddit_stats': all_stats,
        'total_new_images': sum(s['new_images'] for s in all_stats),
        'total_errors': sum(s['download_errors'] for s in all_stats),
    }
    
    # Save log locally
    logs_dir = DATA_DIR / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    
    log_filename = f"daily_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    log_path = logs_dir / log_filename
    
    with open(log_path, 'w') as f:
        json.dump(log_data, f, indent=4)
    
    logger.info("\n" + "="*70)
    logger.info("DAILY IMAGE COLLECTION SUMMARY")
    logger.info("="*70)
    for stats in all_stats:
        logger.info(f"r/{stats['subreddit']}: {stats['new_images']} new images")
    logger.info(f"\nTotal new images: {log_data['total_new_images']}")
    logger.info(f"Total errors: {log_data['total_errors']}")
    logger.info(f"Log saved to: {log_path}")
    logger.info("="*70 + "\n")


if __name__ == "__main__":
    collect_all_subreddit_images_local()