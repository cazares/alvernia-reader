#!/usr/bin/env python3
"""
Test yt-dlp audio extraction endpoint locally and remotely.
"""
import subprocess
import sys
import os

# Add scripts to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "scripts"))

from scripts import step1_fetch


def test_ytdlp_command():
    """Test if yt-dlp is working locally"""
    print("=" * 60)
    print("Testing yt-dlp command...")
    print("=" * 60)

    print(f"\nYTDLP_CMD: {step1_fetch.YTDLP_CMD}")
    print(f"YTDLP_UA: {step1_fetch.YTDLP_UA}")
    print(f"YTDLP_EXTRACTOR_ARGS: {step1_fetch.YTDLP_EXTRACTOR_ARGS}")
    print(f"YTDLP_EXTRA_HEADERS: {step1_fetch.YTDLP_EXTRA_HEADERS}")

    # Test query
    test_query = "the beatles - let it be"
    print(f"\nTest query: {test_query}")

    # Search for video ID
    print("\n1. Searching for video ID...")
    try:
        video_ids = step1_fetch.yt_search_ids(test_query, 1, timeout_sec=15.0)
        if not video_ids:
            print("❌ No results found")
            return False
        video_id = video_ids[0]
        print(f"✅ Found video ID: {video_id}")
    except Exception as e:
        print(f"❌ Search failed: {e}")
        return False

    # Extract audio URL
    print("\n2. Extracting audio URL...")
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    cmd = [*step1_fetch.YTDLP_CMD]
    cmd += [
        "--get-url",
        "--format", "bestaudio[ext=m4a]/bestaudio",
        "--no-playlist",
        "--force-ipv4",
        "--socket-timeout", str(step1_fetch.YTDLP_SOCKET_TIMEOUT),
        "--retries", str(step1_fetch.YTDLP_RETRIES),
    ]

    if step1_fetch.YTDLP_UA:
        cmd += ["--user-agent", str(step1_fetch.YTDLP_UA)]

    for hdr in step1_fetch.YTDLP_EXTRA_HEADERS:
        cmd += ["--add-headers", hdr]

    if step1_fetch.YTDLP_EXTRACTOR_ARGS:
        cmd += ["--extractor-args", str(step1_fetch.YTDLP_EXTRACTOR_ARGS)]

    cmd.append(video_url)

    print(f"\nRunning command:")
    print(f"  {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=30,
        )

        print(f"Return code: {result.returncode}")

        if result.stdout:
            print(f"\nSTDOUT:")
            print(result.stdout)

        if result.stderr:
            print(f"\nSTDERR:")
            print(result.stderr)

        if result.returncode != 0:
            print("\n❌ yt-dlp command failed")
            return False

        audio_url = result.stdout.strip()
        if not audio_url or not audio_url.startswith("http"):
            print(f"\n❌ Invalid audio URL: {audio_url}")
            return False

        print(f"\n✅ Successfully extracted audio URL:")
        print(f"   {audio_url[:100]}...")

    except subprocess.TimeoutExpired:
        print("\n❌ Command timed out")
        return False
    except Exception as e:
        print(f"\n❌ Command failed: {e}")
        return False

    # Get metadata
    print("\n3. Getting metadata...")
    metadata_cmd = [*step1_fetch.YTDLP_CMD]
    metadata_cmd += [
        "--print", "title",
        "--print", "duration",
        "--print", "thumbnail",
        "--no-playlist",
        "--force-ipv4",
        "--socket-timeout", str(step1_fetch.YTDLP_SOCKET_TIMEOUT),
    ]

    if step1_fetch.YTDLP_UA:
        metadata_cmd += ["--user-agent", str(step1_fetch.YTDLP_UA)]

    for hdr in step1_fetch.YTDLP_EXTRA_HEADERS:
        metadata_cmd += ["--add-headers", hdr]

    if step1_fetch.YTDLP_EXTRACTOR_ARGS:
        metadata_cmd += ["--extractor-args", str(step1_fetch.YTDLP_EXTRACTOR_ARGS)]

    metadata_cmd.append(video_url)

    try:
        metadata_result = subprocess.run(
            metadata_cmd,
            capture_output=True,
            text=True,
            timeout=15,
        )

        if metadata_result.returncode == 0:
            lines = metadata_result.stdout.strip().split('\n')
            title = lines[0] if len(lines) > 0 else "Unknown"
            duration = lines[1] if len(lines) > 1 else "Unknown"
            thumbnail = lines[2] if len(lines) > 2 else "Unknown"

            print(f"✅ Metadata:")
            print(f"   Title: {title}")
            print(f"   Duration: {duration}s")
            print(f"   Thumbnail: {thumbnail[:80]}...")
        else:
            print(f"⚠️  Metadata extraction failed (but audio URL worked)")

    except Exception as e:
        print(f"⚠️  Metadata extraction failed: {e}")

    print("\n✅ All tests passed!")
    return True


def test_remote_endpoint():
    """Test the remote API endpoint"""
    print("\n" + "=" * 60)
    print("Testing remote API endpoint...")
    print("=" * 60)

    import requests

    base_url = "https://api.miguelendpoint.com"
    test_query = "the beatles - let it be"

    print(f"\nEndpoint: {base_url}/youtube/audio-url")
    print(f"Query: {test_query}")

    try:
        response = requests.get(
            f"{base_url}/youtube/audio-url",
            params={"q": test_query},
            timeout=30
        )

        print(f"\nStatus code: {response.status_code}")
        print(f"Response: {response.text[:500]}")

        if response.status_code == 200:
            data = response.json()
            print(f"\n✅ Remote API works!")
            print(f"   Title: {data.get('title')}")
            print(f"   Duration: {data.get('duration')}s")
            print(f"   Video ID: {data.get('video_id')}")
            print(f"   Audio URL: {data.get('audio_url', '')[:100]}...")
            return True
        else:
            print(f"\n❌ Remote API failed with status {response.status_code}")
            return False

    except Exception as e:
        print(f"\n❌ Remote API request failed: {e}")
        return False


if __name__ == "__main__":
    print("YouTube Audio URL Extraction Test\n")

    # Test local yt-dlp
    local_ok = test_ytdlp_command()

    # Test remote endpoint
    remote_ok = test_remote_endpoint()

    print("\n" + "=" * 60)
    print("Summary:")
    print("=" * 60)
    print(f"Local yt-dlp: {'✅ PASS' if local_ok else '❌ FAIL'}")
    print(f"Remote API: {'✅ PASS' if remote_ok else '❌ FAIL'}")
    print()

    sys.exit(0 if (local_ok and remote_ok) else 1)
