#!/bin/sh
# Creates release archives.
#
# Copies the source code to a separate directory,
# then compresses the directory in tar.gz and zip.
# Deletes any previous version of the directory and the archives beforehand.

NAME='google-shared-contacts-client'
FILES='COPYING outlook.csv README.txt shared_contacts_profiles.py'

# Go to the script directory.
cd "$(readlink -f "$(dirname "$0")")"

# Copy the files to a separate directory.
rm -rf "${NAME}"
mkdir -p "${NAME}"
cp ${FILES} "${NAME}"

# Delete the old archives
rm -f "${NAME}.tar.gz" "${NAME}.zip"

# Compress the directory in tar.gz and zip.
tar cvzf "${NAME}.tar.gz" --owner=nobody --group=nobody "${NAME}/"
zip -9 -r "${NAME}.zip" "${NAME}/"
