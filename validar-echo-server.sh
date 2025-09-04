#!/bin/bash

TEST_MESSAGE="Hello Echo Server Test 123"


# Server ip is not being used in the application but i added it if in the future we need to use it
read_server_config() {
    # Extract SERVER_IP and SERVER_PORT from config.ini
    SERVER_IP=$(grep "^SERVER_IP" server/config.ini | cut -d'=' -f2 | tr -d ' ')
    SERVER_PORT=$(grep "^SERVER_PORT" server/config.ini | cut -d'=' -f2 | tr -d ' ')
    
    # Validate that we got the values
    if [ -z "$SERVER_IP" ] || [ -z "$SERVER_PORT" ]; then
        echo "Error: Could not read server configuration from server/config.ini"
        echo "Using default server values"
        SERVER_IP="server"
        SERVER_PORT="12345"
    fi
    
    echo "Using server: $SERVER_IP:$SERVER_PORT"
}

test_echo_server() {

    read_server_config
    
    # Run netcat in a temporary container connected to the same network defined in the docker-compose-dev.yaml file
    # Send the test message and capture the response
    RESPONSE=$(docker run --rm \
        --network tp0_testing_net \
        --name echo-test \
        alpine:latest \
        sh -c "echo '$TEST_MESSAGE' | nc $SERVER_IP $SERVER_PORT" 2>/dev/null)
    
    # Check if we got a response and if it matches our test message
    if [ $? -eq 0 ] && [ "$RESPONSE" = "$TEST_MESSAGE" ]; then
        echo "action: test_echo_server | result: success"
        return 0
    else
        echo "action: test_echo_server | result: fail"
        return 1
    fi
}

# Main execution
echo "Testing echo server with message: '$TEST_MESSAGE'"
test_echo_server
