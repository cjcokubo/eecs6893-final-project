"""
Apache Airflow DAG for GCP VM
Daily collection of new images from Reddit subreddits to GCS bucket
Optimized for running on Google Cloud Platform VM
"""

from datetime import datetime, timedelta
import pandas as pd
import praw
from pathlib import Path
import json
import requests
from google.cloud import storage
import os
import logging
import tempfile

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.decorators import apply_defaults

####################################################
# CONFIGURATION
####################################################

DALLE_SUBR = "dalle2"
MIDJ_SUBR = "midjourney"
AIART_SUBR = "aiArt"
SUBREDDITS = [DALLE_SUBR, MIDJ_SUBR, AIART_SUBR]

# GCS Configuration
BUCKET_NAME = "eecs6893-bkt"  # Update with your actual bucket name
GCS_DATA_DIR = "reddit_images"

# Reddit API Configuration (from your extract_data.ipynb)
REDDIT_CLIENT_ID = "JT_iiB-NFwFyoknGwv5fYA"
REDDIT_CLIENT_SECRET = "aVY6bitDc5BS8j4LX1cWusWjlgCaVQ"
REDDIT_USER_AGENT = "mac:eecs6893Project:v1.0 (by u/Superb-Cap8523)"

# Flairs from your extract_data.ipynb
FLAIR_DICT = {
    MIDJ_SUBR: set(['AI Showcase - Midjourney']),
    DALLE_SUBR: set(['DALL·E 2', 'DALL·E 3']),
    AIART_SUBR: set([
        'Image -  I used x/ai\'s "Imagine Image and Video Generator."  ',
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
        'Image - etana'
    ])
}

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

####################################################
# GCP-SPECIFIC HELPER FUNCTIONS
####################################################

def get_gcs_client():
    """
    Initialize GCS client
    Assumes VM has default credentials set up
    (Compute Engine service account or Application Default Credentials)
    """
    try:
        client = storage.Client()
        logger.info("✓ GCS client initialized successfully")
        return client
    except Exception as e:
        logger.error(f"❌ Failed to initialize GCS client: {e}")
        raise

def get_existing_image_ids_from_gcs(bucket_name, subr):
    """
    Get all existing image IDs from GCS bucket
    
    Args:
        bucket_name: GCS bucket name
        subr: subreddit name
    
    Returns:
        Set of submission IDs already downloaded
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        
        existing_ids = set()
        prefix = f"{GCS_DATA_DIR}/{subr}/"
        
        # List all blobs in the subreddit folder
        blobs = bucket.list_blobs(prefix=prefix)
        
        for blob in blobs:
            # Skip metadata files
            if blob.name.endswith('metadata.json'):
                continue
            
            # Extract submission ID from blob name
            # Format: reddit_images/{subr}/img_{subr}_{submission_id}.{ext}
            filename = blob.name.split('/')[-1]
            
            if filename.startswith('img_'):
                parts = filename.split('_')
                if len(parts) >= 3:
                    submission_id = "_".join(parts[2:]).split('.')[0]
                    existing_ids.add(submission_id)
        
        logger.info(f"Found {len(existing_ids)} existing images for r/{subr} in GCS")
        return existing_ids
    
    except Exception as e:
        logger.error(f"Error getting existing IDs from GCS: {e}")
        raise

def upload_to_gcs(bucket_name, local_file_path, gcs_file_path):
    """
    Upload file to GCS with retries
    
    Args:
        bucket_name: GCS bucket name
        local_file_path: Local file path
        gcs_file_path: Target GCS path (without gs:// prefix)
    
    Returns:
        True if successful
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        
        # Upload with timeout
        blob.upload_from_filename(local_file_path, timeout=300)
        logger.info(f"✓ Uploaded to gs://{bucket_name}/{gcs_file_path}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Error uploading to GCS: {e}")
        return False

def download_from_gcs(bucket_name, gcs_file_path, local_file_path):
    """
    Download file from GCS
    
    Args:
        bucket_name: GCS bucket name
        gcs_file_path: GCS file path (without gs:// prefix)
        local_file_path: Local destination
    
    Returns:
        True if successful
    """
    try:
        client = get_gcs_client()
        bucket = client.bucket(bucket_name)
        blob = bucket.blob(gcs_file_path)
        
        # Check if blob exists
        if not blob.exists():
            logger.warning(f"File does not exist in GCS: {gcs_file_path}")
            return False
        
        blob.download_to_filename(local_file_path, timeout=300)
        logger.info(f"✓ Downloaded gs://{bucket_name}/{gcs_file_path}")
        return True
    
    except Exception as e:
        logger.error(f"❌ Error downloading from GCS: {e}")
        return False

def get_file_extension(url):
    """Extract file extension from URL"""
    from urllib.parse import urlparse
    
    parsed_url = urlparse(url)
    _, ext = os.path.splitext(parsed_url.path)
    
    return ext.lower() if ext else '.jpg'

####################################################
# MAIN COLLECTION FUNCTIONS
####################################################

def collect_new_images_from_subreddit(subr, flair):
    """
    Collect new images from a subreddit and upload to GCS
    
    Args:
        subr: Subreddit name
        flair: Set of valid flairs
    
    Returns:
        Dict with collection statistics
    """
    
    logger.info(f"\n{'='*70}")
    logger.info(f"Collecting new images from r/{subr}")
    logger.info(f"{'='*70}")
    
    try:
        # Initialize Reddit client
        reddit = praw.Reddit(
            client_id=REDDIT_CLIENT_ID,
            client_secret=REDDIT_CLIENT_SECRET,
            user_agent=REDDIT_USER_AGENT
        )
        logger.info("✓ Reddit client initialized")
    except Exception as e:
        logger.error(f"❌ Failed to initialize Reddit client: {e}")
        raise
    
    # Get existing image IDs from GCS
    try:
        existing_ids = get_existing_image_ids_from_gcs(BUCKET_NAME, subr)
    except Exception as e:
        logger.error(f"❌ Failed to get existing IDs: {e}")
        raise
    
    # Collect new posts
    try:
        subreddit = reddit.subreddit(subr)
    except Exception as e:
        logger.error(f"❌ Failed to access subreddit: {e}")
        raise
    
    new_images = []
    stats = {
        'subreddit': subr,
        'checked': 0,
        'passed_flair': 0,
        'passed_image': 0,
        'passed_nsfw': 0,
        'new_images': 0,
        'download_errors': 0,
        'upload_errors': 0,
        'images': []
    }
    
    image_extensions = ('.jpg', '.jpeg', '.png')
    
    # Use temporary directory for intermediate files
    with tempfile.TemporaryDirectory() as tmp_dir:
        tmp_path = Path(tmp_dir)
        
        # Check most recent 500 posts
        try:
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
                
                # This is a NEW image! Download and upload to GCS
                try:
                    # Download image
                    response = requests.get(submission.url, timeout=10)
                    
                    # Get original extension
                    original_ext = get_file_extension(submission.url)
                    filename = f"img_{subr}_{submission.id}{original_ext}"
                    
                    # Save to temporary directory
                    local_path = tmp_path / filename
                    with open(local_path, 'wb') as f:
                        f.write(response.content)
                    
                    # Upload to GCS
                    gcs_path = f"{GCS_DATA_DIR}/{subr}/{filename}"
                    if upload_to_gcs(BUCKET_NAME, str(local_path), gcs_path):
                        stats['new_images'] += 1
                        new_images.append({
                            'filename': filename,
                            'submission_id': submission.id,
                            'url': submission.url,
                            'flair': submission.link_flair_text,
                            'created_at': datetime.fromtimestamp(submission.created_utc).isoformat(),
                            'score': submission.score,
                        })
                        logger.info(f"✓ Downloaded and uploaded: {filename}")
                    else:
                        stats['upload_errors'] += 1
                
                except requests.Timeout:
                    stats['download_errors'] += 1
                    logger.error(f"✗ Timeout downloading {submission.url}")
                except Exception as e:
                    stats['download_errors'] += 1
                    logger.error(f"✗ Error downloading {submission.url}: {e}")
        
        except Exception as e:
            logger.error(f"❌ Error iterating through posts: {e}")
            raise
    
    stats['images'] = new_images
    
    # Log summary
    logger.info(f"\nProcessing summary for r/{subr}:")
    logger.info(f"  Total posts checked: {stats['checked']}")
    logger.info(f"  Passed flair filter: {stats['passed_flair']}")
    logger.info(f"  Passed image filter: {stats['passed_image']}")
    logger.info(f"  Passed NSFW filter: {stats['passed_nsfw']}")
    logger.info(f"  🆕 New images: {stats['new_images']}")
    logger.info(f"  ❌ Download errors: {stats['download_errors']}")
    logger.info(f"  ⚠️  Upload errors: {stats['upload_errors']}")
    
    return stats

def update_metadata_json_gcs(bucket_name, subr, new_images):
    """
    Update metadata.json for dalle2 and aiArt subreddits in GCS
    
    Args:
        bucket_name: GCS bucket name
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
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            
            # Download existing metadata from GCS
            metadata_gcs_path = f"{GCS_DATA_DIR}/{subr}/metadata.json"
            metadata_local_path = tmp_path / f"{subr}_metadata.json"
            
            # Load existing metadata or create new dict
            if download_from_gcs(bucket_name, metadata_gcs_path, str(metadata_local_path)):
                with open(metadata_local_path, 'r') as f:
                    metadata = json.load(f)
                logger.info(f"Loaded existing metadata for r/{subr}")
            else:
                metadata = {}
                logger.info(f"Creating new metadata for r/{subr}")
            
            # Add new images to metadata
            for img in new_images:
                img_gcs_path = f"{GCS_DATA_DIR}/{subr}/{img['filename']}"
                metadata[img_gcs_path] = {
                    'flair': img['flair'],
                    'url': img['url'],
                    'created_at': img['created_at'],
                    'score': img['score'],
                }
            
            # Save updated metadata locally
            with open(metadata_local_path, 'w') as f:
                json.dump(metadata, f, indent=4)
            
            # Upload updated metadata back to GCS
            if upload_to_gcs(bucket_name, str(metadata_local_path), metadata_gcs_path):
                logger.info(f"✓ Updated metadata.json for r/{subr} with {len(new_images)} new entries")
            else:
                logger.error(f"❌ Failed to upload metadata for r/{subr}")
    
    except Exception as e:
        logger.error(f"❌ Error updating metadata for r/{subr}: {e}")

def collect_all_subreddit_images():
    """
    Main function: Collect images from all subreddits
    Called by Airflow daily
    """
    
    logger.info("\n" + "="*70)
    logger.info(f"DAILY IMAGE COLLECTION STARTED - {datetime.now()}")
    logger.info(f"GCS Bucket: {BUCKET_NAME}")
    logger.info("="*70)
    
    all_stats = []
    
    # Collect from each subreddit
    for subr in SUBREDDITS:
        try:
            stats = collect_new_images_from_subreddit(subr, FLAIR_DICT[subr])
            all_stats.append(stats)
            
            # Update metadata for dalle2 and aiArt
            if subr in [DALLE_SUBR, AIART_SUBR]:
                update_metadata_json_gcs(BUCKET_NAME, subr, stats['images'])
        
        except Exception as e:
            logger.error(f"❌ Failed to collect from r/{subr}: {e}")
            all_stats.append({
                'subreddit': subr,
                'error': str(e),
                'new_images': 0,
            })
    
    # Create daily log
    log_data = {
        'date': datetime.now().isoformat(),
        'subreddit_stats': all_stats,
        'total_new_images': sum(s.get('new_images', 0) for s in all_stats),
        'total_errors': sum(s.get('download_errors', 0) + s.get('upload_errors', 0) for s in all_stats if isinstance(s, dict) and 'download_errors' in s),
    }
    
    # Save log to GCS
    try:
        with tempfile.TemporaryDirectory() as tmp_dir:
            tmp_path = Path(tmp_dir)
            log_local_path = tmp_path / f"daily_collection_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            
            with open(log_local_path, 'w') as f:
                json.dump(log_data, f, indent=4)
            
            log_gcs_path = f"{GCS_DATA_DIR}/logs/daily_collection_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
            upload_to_gcs(BUCKET_NAME, str(log_local_path), log_gcs_path)
    
    except Exception as e:
        logger.error(f"❌ Error saving log: {e}")
    
    logger.info("\n" + "="*70)
    logger.info("DAILY IMAGE COLLECTION SUMMARY")
    logger.info("="*70)
    for stats in all_stats:
        if 'error' in stats:
            logger.error(f"r/{stats['subreddit']}: ERROR - {stats['error']}")
        else:
            logger.info(f"r/{stats['subreddit']}: {stats['new_images']} new images")
    logger.info(f"\nTotal new images: {log_data['total_new_images']}")
    logger.info(f"Total errors: {log_data['total_errors']}")
    logger.info("="*70 + "\n")

####################################################
# AIRFLOW DAG DEFINITION
####################################################

default_args = {
    'owner': 'reddit-image-collector',
    'depends_on_past': False,
    'email': ['your-email@example.com'],  # Update with your email
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=10),
    'execution_timeout': timedelta(hours=2),  # Timeout after 2 hours
}

with DAG(
    'daily_reddit_image_collection_gcp',
    default_args=default_args,
    description='Daily collection of new images from Reddit AI art subreddits to GCS',
    schedule_interval='0 9 * * *',  # 9 AM UTC every day
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['reddit', 'image-collection', 'ai-art', 'gcp'],
) as dag:

    ##########################################
    # TASK DEFINITION
    ##########################################

    collect_images_task = PythonOperator(
        task_id='collect_new_images_to_gcs',
        python_callable=collect_all_subreddit_images,
        retries=2,
        pool='default_pool',
    )

    # Run the task
    collect_images_task