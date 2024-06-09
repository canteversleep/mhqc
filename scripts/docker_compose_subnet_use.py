import yaml

# Load the existing docker-compose.yml file
with open('docker-compose.yml', 'r') as file:
    compose_data = yaml.safe_load(file)

# Define the network configuration
network_config = {
    'networks': {
        'default': {
            'ipam': {
                'driver': 'default',
                'config': [
                    {'subnet': '172.22.0.0/16'}
                ]
            }
        }
    }
}

# Update the compose data with the network configuration
compose_data.update(network_config)

# Update each service to use the default network
for service_name in compose_data['services']:
    service = compose_data['services'][service_name]
    if 'networks' not in service:
        service['networks'] = []
    service['networks'].append('default')

# Write the updated compose data back to the file
with open('docker-compose.yml', 'w') as file:
    yaml.dump(compose_data, file)

print("docker-compose.yml file updated successfully.")
