#!/bin/bash
# Test the deployed API endpoint

echo "Testing deployed API at api.miguelendpoint.com..."
echo ""

# Test 1: Health check
echo "1. Health check:"
curl -s "https://api.miguelendpoint.com/health" | jq '.' || echo "Health check failed"
echo ""

# Test 2: YouTube audio URL extraction
echo "2. Testing YouTube audio URL extraction:"
echo "   Query: the beatles - let it be"
response=$(curl -s "https://api.miguelendpoint.com/youtube/audio-url?q=the%20beatles%20-%20let%20it%20be")
echo "$response" | jq '.' 2>/dev/null || echo "$response"
echo ""

# Check if successful
if echo "$response" | jq -e '.audio_url' > /dev/null 2>&1; then
    echo "✅ SUCCESS! Audio URL extraction is working!"
    title=$(echo "$response" | jq -r '.title')
    video_id=$(echo "$response" | jq -r '.video_id')
    duration=$(echo "$response" | jq -r '.duration')
    echo "   Title: $title"
    echo "   Video ID: $video_id"
    echo "   Duration: ${duration}s"
else
    echo "❌ FAILED - API returned error"
    echo "Check the error message above for details"
fi
