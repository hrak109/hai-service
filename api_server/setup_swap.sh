#!/bin/bash
set -e

echo "Checking for existing swap..."
if grep -q "swap" /proc/swaps; then
    echo "Swap already exists. Skipping creation."
    free -h
    exit 0
fi

echo "Creating 2GB swap file..."
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile

echo "Persisting swap in /etc/fstab..."
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab

echo "Swap created successfully!"
free -h
