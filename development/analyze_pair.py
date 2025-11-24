import os
import sys
import requests
import json
import numpy as np
import matplotlib.pyplot as plt
from typing import Dict, List, Tuple

# Add current directory to path to find snowflake_utils
# Add parent directory to path to find snowflake_utils
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

try:
    from snowflake_utils import SnowflakeConnector
    HAS_SNOWFLAKE = True
except ImportError:
    HAS_SNOWFLAKE = False
    print("⚠️  snowflake_utils not found or dependencies missing.")

# Try to import audio analysis libraries
try:
    import librosa
    import librosa.display
    HAS_LIBROSA = True
except ImportError:
    HAS_LIBROSA = False
    print("⚠️  librosa not found. Falling back to basic analysis or requesting install.")

def fetch_file_keys_from_snowflake(ids: List[int]) -> Dict[int, str]:
    """Fetch file keys from Snowflake"""
    if not HAS_SNOWFLAKE:
        print("❌ Snowflake connector not available.")
        return {}
        
    print(f"Fetching file keys from Snowflake for IDs: {ids}...")
    try:
        snowflake = SnowflakeConnector()
        ids_str = ", ".join(map(str, ids))
        query = f"""
        SELECT ASSET_ID, FILE_KEY 
        FROM AI_DATA.AUDIO_FINGERPRINT 
        WHERE ASSET_ID IN ({ids_str})
        AND PROCESSING_STATUS = 'SUCCESS'
        """
        
        cursor = snowflake.execute_query(query)
        result = {}
        for row in cursor:
            asset_id = int(row[0])
            file_key = row[1]
            # Just take the first one found for each asset
            if asset_id not in result:
                result[asset_id] = file_key
        
        snowflake.close()
        return result
    except Exception as e:
        print(f"❌ Error fetching from Snowflake: {e}")
        return {}

def get_download_url(keys: List[str]) -> Dict[str, str]:
    """Get signed download URLs"""
    api_url = "https://oapi-int.artlist.io/v1/content/bulkDownloadArtifacts"
    headers = {
        'Content-Type': 'application/json',
        'service-host': 'core.content.cms.api'
    }
    
    try:
        response = requests.post(api_url, headers=headers, json={"keys": keys})
        response.raise_for_status()
        data = response.json()
        
        urls = {}
        if 'data' in data and 'downloadArtifactResponses' in data['data']:
            responses = data['data']['downloadArtifactResponses']
            # Map back by index since the API returns a map by index string "0", "1", etc.
            for i, key in enumerate(keys):
                idx_str = str(i)
                if idx_str in responses:
                    urls[key] = responses[idx_str].get('url')
        return urls
    except Exception as e:
        print(f"❌ Error fetching download URLs: {e}")
        return {}

def download_song(song_id: int, file_key: str, output_dir: str) -> str:
    """Download song to file"""
    urls = get_download_url([file_key])
    url = urls.get(file_key)
    
    if not url:
        print(f"❌ No URL found for {song_id}")
        return None
        
    filename = os.path.join(output_dir, f"{song_id}.mp3")
    
    if os.path.exists(filename):
        print(f"✅ File already exists: {filename}")
        return filename
        
    print(f"⬇️  Downloading {song_id}...")
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        with open(filename, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        return filename
    except Exception as e:
        print(f"❌ Download failed: {e}")
        return None

def analyze_pair(input1, input2):
    """Analyze and compare two songs (IDs or file paths)"""
    output_dir = "analysis_output"
    os.makedirs(output_dir, exist_ok=True)
    
    file1 = None
    file2 = None
    label1 = str(input1)
    label2 = str(input2)

    # Check if inputs are existing file paths
    if isinstance(input1, str) and os.path.exists(input1):
        print(f"🔍 Using local file 1: {input1}")
        file1 = input1
        label1 = os.path.basename(input1)
    
    if isinstance(input2, str) and os.path.exists(input2):
        print(f"🔍 Using local file 2: {input2}")
        file2 = input2
        label2 = os.path.basename(input2)
        
    # If not local files, treat as IDs and download
    if not file1 or not file2:
        try:
            id1 = int(input1) if not file1 else None
            id2 = int(input2) if not file2 else None
            
            ids_to_fetch = []
            if id1: ids_to_fetch.append(id1)
            if id2: ids_to_fetch.append(id2)
            
            if ids_to_fetch:
                print(f"🔍 Step 1: Fetching metadata for IDs {ids_to_fetch}...")
                metadata = fetch_file_keys_from_snowflake(ids_to_fetch)
                
                if id1 and id1 not in metadata:
                    print(f"❌ Could not find file key for {id1}")
                    return
                if id2 and id2 not in metadata:
                    print(f"❌ Could not find file key for {id2}")
                    return
                    
                print(f"🔍 Step 2: Downloading songs...")
                if id1: file1 = download_song(id1, metadata[id1], output_dir)
                if id2: file2 = download_song(id2, metadata[id2], output_dir)
        except ValueError:
            if not file1: print(f"❌ Input '{input1}' is neither a file nor an integer ID")
            if not file2: print(f"❌ Input '{input2}' is neither a file nor an integer ID")
            return

    if not file1 or not file2:
        print("❌ Failed to obtain audio files.")
        return

    # 3. Analyze
    if not HAS_LIBROSA:
        print("❌ librosa library is required for analysis.")
        return

    print(f"🔍 Step 3: Analyzing audio...")
    
    # 3. Analyze
    if not HAS_LIBROSA:
        print("❌ librosa library is required for analysis.")
        return

    print(f"🔍 Step 3: Analyzing audio...")
    
    # Load FULL audio to find difference
    # Use mono=True to simplify comparison
    y1, sr1 = librosa.load(file1, sr=None, mono=True)
    y2, sr2 = librosa.load(file2, sr=None, mono=True)
    
    # Ensure same length
    min_len = min(len(y1), len(y2))
    y1 = y1[:min_len]
    y2 = y2[:min_len]
    
    # Find first point of difference
    diff = np.abs(y1 - y2)
    threshold = 0.01 # Audible difference threshold
    diff_indices = np.where(diff > threshold)[0]
    
    if len(diff_indices) > 0:
        first_diff_sample = diff_indices[0]
        first_diff_time = first_diff_sample / sr1
        print(f"❗ DIFFERENCE FOUND starting at {first_diff_time:.2f} seconds!")
        
        # Set plot window around the difference
        start_sample = max(0, first_diff_sample - sr1*2) # 2 seconds before
        end_sample = min(min_len, first_diff_sample + sr1*5) # 5 seconds after
        
        y1_plot = y1[start_sample:end_sample]
        y2_plot = y2[start_sample:end_sample]
        diff_plot = diff[start_sample:end_sample]
        
        plot_title_suffix = f" (Zoomed at {first_diff_time:.2f}s)"
    else:
        print("✅ No significant difference found in the entire file.")
        # Just plot the beginning
        start_sample = 0
        end_sample = sr1 * 10
        y1_plot = y1[start_sample:end_sample]
        y2_plot = y2[start_sample:end_sample]
        diff_plot = diff[start_sample:end_sample]
        plot_title_suffix = " (First 10s)"

    # 4. Visualize
    print(f"🔍 Step 4: Generating comparison plots...")
    
    plt.figure(figsize=(15, 12))
    
    # Waveforms
    plt.subplot(3, 1, 1)
    time_axis = np.linspace(start_sample/sr1, end_sample/sr1, len(y1_plot))
    plt.plot(time_axis, y1_plot, alpha=0.6, label=label1)
    plt.plot(time_axis, y2_plot, alpha=0.6, label=label2, color='r')
    plt.title(f'Waveform Overlay{plot_title_suffix}')
    plt.legend()
    plt.xlabel('Time (s)')
    
    # Spectrograms (of the specific region)
    D1 = librosa.amplitude_to_db(np.abs(librosa.stft(y1_plot)), ref=np.max)
    D2 = librosa.amplitude_to_db(np.abs(librosa.stft(y2_plot)), ref=np.max)
    
    plt.subplot(3, 2, 3)
    librosa.display.specshow(D1, sr=sr1, x_axis='time', y_axis='log')
    plt.colorbar(format='%+2.0f dB')
    plt.title(f'Spectrogram {label1}')
    
    plt.subplot(3, 2, 4)
    librosa.display.specshow(D2, sr=sr2, x_axis='time', y_axis='log')
    plt.colorbar(format='%+2.0f dB')
    plt.title(f'Spectrogram {label2}')
    
    # Difference
    plt.subplot(3, 1, 3)
    plt.plot(time_axis, diff_plot, color='g')
    plt.title('Absolute Difference')
    plt.xlabel('Time (s)')
    
    # Create safe filename
    safe_label1 = "".join([c for c in label1 if c.isalnum() or c in ('-','_')]).rstrip()
    safe_label2 = "".join([c for c in label2 if c.isalnum() or c in ('-','_')]).rstrip()
    output_plot = os.path.join(output_dir, f"comparison_diff.png")
    
    plt.tight_layout()
    plt.savefig(output_plot)
    print(f"✅ Plot saved to: {output_plot}")
    
    # 5. Calculate Similarity (Global)
    correlation = np.corrcoef(y1, y2)[0, 1]
    max_diff = np.max(diff)
    
    print(f"\n📊 Similarity Metrics (Global):")
    print(f"   Waveform Correlation: {correlation:.6f}")
    print(f"   Max Absolute Difference: {max_diff:.6f}")
    
    if max_diff < 1e-4:
         print("   👉 Note: Audio is effectively identical.")
    else:
         print("   👉 Conclusion: DIFFERENT (Files diverge)")

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python analyze_pair.py <id1/file1> <id2/file2>")
        print("Running with default IDs: 84470 84471")
        analyze_pair(84470, 84471)
    else:
        # Try to parse as int, otherwise keep as string
        arg1 = sys.argv[1]
        arg2 = sys.argv[2]
        analyze_pair(arg1, arg2)
