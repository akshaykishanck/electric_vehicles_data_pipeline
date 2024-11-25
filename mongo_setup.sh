#!/bin/bash

# Update and install MongoDB
echo "Updating package lists and installing MongoDB..."
sudo apt update
sudo apt-get install -y mongodb-org

# Start MongoDB service
echo "Starting MongoDB service..."
sudo systemctl start mongod

# Enable MongoDB to start on boot
echo "Enabling MongoDB service on boot..."
sudo systemctl enable mongod

# Check MongoDB service status
echo "Checking MongoDB status..."
sudo systemctl status mongod --no-pager

# Confirm installation
echo "MongoDB setup complete!"
