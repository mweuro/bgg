#!/bin/bash

# Load environment variables
source .env.bgg

# Download ZIP
curl -L -H "Cookie: $COOKIE_HEADER" -o "$ZIP_FILE" "$ZIP_URL"

if [ $? -ne 0 ]; then
    echo "Error downloading the ZIP file."
    exit 1

else
    echo "ZIP file downloaded successfully."

    # Unpack ZIP
    unzip -o "$ZIP_FILE" -d ./

    # Get CSV file name from ZIP
    CSV_FILE=$(unzip -l "$ZIP_FILE" | awk 'NR==4{print $4}')

    # Remove ZIP
    rm "$ZIP_FILE"

    # Ensure destination directory exists
    mkdir -p "$DIR_DESTINATION"

    # Extract first column from CSV to TXT
    tail -n +2 "$CSV_FILE" | cut -d',' -f1 > "$TXT_FILE"

    # Remove CSV
    rm "$CSV_FILE"

    echo "Extracted first column to $TXT_FILE"
fi


