#!/bin/bash
set -e

echo "Checking for existing swap..."
if grep -q "swap" /proc/swaps; then
    echo "Swap already exists. Skipping creation."
    free -h
    exit 0
fi

echo "Creating 2GB swap file using dd (this may take a minute)..."
# dd is safer than fallocate for swap files as it strictly allocates disk blocks
if ! sudo dd if=/dev/zero of=/swapfile bs=1M count=2048; then
    echo "Error: Failed to create swapfile"
    exit 1
fi

sudo chmod 600 /swapfile
sudo mkswap /swapfile
if ! sudo swapon /swapfile; then
    echo "Error: Failed to enable swap"
    exit 1
fi

echo "Persisting swap in /etc/fstab..."
# Check if entry already exists to avoid duplicates
if ! grep -q "/swapfile none swap" /etc/fstab; then
    echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
fi

echo "Swap created successfully!"
free -h
