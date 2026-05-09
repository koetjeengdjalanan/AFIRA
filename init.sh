#!/bin/sh

# Check if .env file exists, if not create it from .env.example
if [ ! -f .env ]; then
    echo "Creating .env file from .env.example..."
    cp .env.example .env
    echo ".env file created. Please review and update it with your actual configuration."
else
    echo ".env file already exists. Skipping creation."
fi

# Check if apps directories exists, if not create them
if [ ! -d "./logs" ]; then
    echo "Creating logs directory..."
    mkdir ./logs
fi

if [ ! -d "./outputs" ]; then
    echo "Creating outputs directory..."
    mkdir ./outputs
fi