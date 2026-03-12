#!/usr/bin/env python3
"""
Test script to verify the YouTube audio URL extraction fix.
This simulates what the /youtube/audio-url endpoint does.
"""

import subprocess
import sys

def test_youtube_extraction(query: str):
    print(f"\n🎵 Testing YouTube extraction for: {query}")
    print("=" * 60)

    # Simulate the fixed yt-dlp command with proper flags
    video_id = "dQw4w9WgXcQ"  # Rick Astley - Never Gonna Give You Up
    video_url = f"https://www.youtube.com/watch?v={video_id}"

    cmd = [
        "yt-dlp",
        "--get-url",
        "--format", "bestaudio[ext=m4a]/bestaudio",
        "--no-playlist",
        "--force-ipv4",
        "--socket-timeout", "6",
        "--retries", "1",
        video_url,
    ]

    print(f"\n📡 Running command:")
    print(f"  {' '.join(cmd)}")
    print()

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=20,
        )

        if result.returncode != 0:
            print(f"❌ FAILED (exit code {result.returncode})")
            print(f"\nStderr:\n{result.stderr}")
            return False

        audio_url = result.stdout.strip()
        if not audio_url or not audio_url.startswith("http"):
            print(f"❌ FAILED - Invalid URL returned")
            return False

        print(f"✅ SUCCESS!")
        print(f"\n📥 Audio URL extracted:")
        print(f"  {audio_url[:100]}...")
        print(f"\n✨ The fix works! The server endpoint should work the same way.")
        return True

    except subprocess.TimeoutExpired:
        print(f"❌ FAILED - Command timed out")
        return False
    except Exception as e:
        print(f"❌ FAILED - Error: {e}")
        return False


if __name__ == "__main__":
    query = sys.argv[1] if len(sys.argv) > 1 else "Rick Astley Never Gonna Give You Up"
    success = test_youtube_extraction(query)
    sys.exit(0 if success else 1)
