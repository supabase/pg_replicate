#!/bin/bash

# Function to run the test with timeout
run_test() {
    # Try to use gtimeout if available (from coreutils)
    if command -v gtimeout &> /dev/null; then
        gtimeout 10 cargo test --package etl --test mod integration::pipeline_v2_test
    else
        # Fallback to perl for timeout
        perl -e 'alarm 10; exec @ARGV' "cargo" "test" "--package" "etl" "--test" "mod" "integration::pipeline_v2_test"
    fi
    return $?
}

# Main loop
while true; do
    echo "Starting test run..."
    run_test
    exit_code=$?
    
    if [ $exit_code -eq 124 ] || [ $exit_code -eq 142 ]; then
        echo "Test stalled (timeout after 10 seconds). Stopping..."
        break
    elif [ $exit_code -eq 0 ]; then
        echo "Test completed successfully!"
        rm -rf etl/logs
        echo "Cleaned up logs directory"
    else
        echo "Test failed with exit code $exit_code"
        rm -rf etl/logs
        echo "Cleaned up logs directory"
    fi
done 