#!/bin/bash
sudo apt-get install gnupg curl

curl -fsSL https://www.mongodb.org/static/pgp/server-8.0.asc | \
   sudo gpg -o /usr/share/keyrings/mongodb-server-8.0.gpg \
   --dearmor

echo "deb [ signed-by=/usr/share/keyrings/mongodb-server-8.0.gpg ] http://repo.mongodb.org/apt/debian bookworm/mongodb-org/8.0 main" | sudo tee /etc/apt/sources.list.d/mongodb-org-8.0.list

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
