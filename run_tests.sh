#!/bin/bash

# Function to run the test with timeout
run_tests() {
    gtimeout 10s cargo test --package etl --test mod integration::pipeline_v2_test
    return $?
}

# Function to clean up logs
cleanup_logs() {
    echo "Attempting to clean up logs directory..."
    if [ -d "etl/logs" ]; then
        # First try normal removal
        if rm -rf etl/logs 2>/dev/null; then
            echo "Successfully cleaned up logs directory"
        else
            # If that fails, try with sudo
            echo "Normal removal failed, trying with elevated permissions..."
            if sudo rm -rf etl/logs 2>/dev/null; then
                echo "Successfully cleaned up logs directory with elevated permissions"
            else
                echo "Warning: Failed to clean up logs directory. You may need to clean it manually."
            fi
        fi
    else
        echo "Logs directory does not exist, no cleanup needed"
    fi
}

# Main loop
while true; do
    echo "Starting test run..."
    run_tests
    exit_code=$?

    if [ $exit_code -eq 124 ] || [ $exit_code -eq 142 ]; then
        echo "Test stalled (timeout after 10 seconds). Stopping..."
        break
    elif [ $exit_code -eq 0 ]; then
        echo "Test completed successfully!"
        cleanup_logs
    else
        echo "Test failed with exit code $exit_code"
        cleanup_logs
    fi
done