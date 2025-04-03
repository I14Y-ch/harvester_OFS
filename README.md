# I14Y Harvester

## Project Structure

For each agent:

- .github/workflows/[agent-workflow].yml: GitHub Actions workflow file for the specific organization and purpose
- src/harvester_ABN.py: Main Python script for data harvesting
- src/config.py: configuration file
- src/dcat_properties_utils.py: python script containing the data extracting and mapping functions
- src/mapping.py: mapping disctonary to standardize some properties

## Workflow

- The frequency at which the workflow runs is defined in the corresponding yml file.
- It can also be triggered or disabled manually from the Actions tab.
- After each run, a log file is generated and uploaded as an artifact, which can be downloaded from the Actions tab.

