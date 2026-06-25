import yaml
import sys
import argparse
import os

# The services we want to replace with remote images
SERVICES_TO_REPLACE = {
    'connect': 'connect',
    'bms-simulator': 'bms-simulator',
    'ems-simulator': 'ems-simulator',
    'bms-gateway': 'bms-gateway',
    'ems-gateway': 'ems-gateway',
    'erp-api': 'erp-api',
    'flink-processor': 'flink-processor'
}

def generate_release_compose(input_path, output_path, tag, registry_base):
    """
    Reads a docker-compose file and replaces 'build' sections for specified
    services with 'image' pointing to the registry.
    """
    if not os.path.exists(input_path):
        print(f"Error: Input file {input_path} not found.")
        sys.exit(1)

    with open(input_path, 'r') as f:
        # We use FullLoader to handle anchors/aliases like <<: *common
        try:
            compose_data = yaml.load(f, Loader=yaml.FullLoader)
        except Exception as e:
            print(f"Error parsing YAML: {e}")
            sys.exit(1)

    if not compose_data or 'services' not in compose_data:
        print("Error: Invalid docker-compose format (missing 'services').")
        sys.exit(1)

    services = compose_data['services']
    replaced_count = 0

    for service_name, image_suffix in SERVICES_TO_REPLACE.items():
        if service_name in services:
            service_config = services[service_name]

            # Check if 'build' key exists in this service
            if 'build' in service_config:
                # Construct the new image path
                new_image = f"{registry_base}/{image_suffix}:{tag}"

                # Remove 'build' and add 'image'
                del service_config['build']
                service_config['image'] = new_image
                replaced_count += 1
                print(f"Replaced build for '{service_name}' with image: {new_image}")
            else:
                print(f"Warning: Service '{service_name}' found but no 'build' block to replace.")

    # Write the modified YAML
    with open(output_path, 'w') as f:
        # Using default_flow_style=False to keep it human-readable (block style)
        # sort_keys=False to preserve the original order as much as possible
        yaml.dump(compose_data, f, default_flow_style=False, sort_keys=False)

    print(f"Successfully generated {output_path}. Replaced {replaced_count} services.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate a release docker-compose file.")
    parser.add_argument("input", help="Input docker-compose file path")
    parser.add_argument("output", help="Output docker-compose file path")
    parser.add_argument("--tag", required=True, help="Docker image tag to use")
    parser.add_argument("--registry", required=True, help="Registry base URL (e.g., ghcr.io/owner/repo)")

    args = parser.parse_args()
    generate_release_compose(args.input, args.output, args.tag, args.registry)
