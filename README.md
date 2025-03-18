# Debt Customer Details Processor

## Description

This project fetches debt customer details from a MySQL database, processes the data based on configurable time parameters, and updates the configuration table with the latest processing time. The goal is to ensure that data is processed only when needed based on configurable time windows.

## Features

- Fetch debt customer data based on configurable time intervals.
- Update the configuration table after processing.
- Detailed logging to track process execution.

## Prerequisites

- Python 3.x
- MySQL database (with phpMyAdmin or similar for access)
- Required Python libraries (listed in `requirements.txt`)

## Installation

1. Clone the repository:
    ```
    git clone https://github.com/your-username/project-name.git
    cd project-name
    ```

2. Set up a virtual environment:
    - On Windows:
      ```
      python -m venv venv
      .\venv\Scripts\activate
      ```
    - On macOS/Linux:
      ```
      python -m venv venv
      source venv/bin/activate
      ```

3. Install dependencies:
    ```
    pip install -r requirements.txt
    ```

4. Configure Database Credentials in `src/db_connection.py`.

5. Configure Logging in the `loggers.ini` file.

## Usage

To run the project, simply execute:
```
python main.py
```

The script will fetch debt customer data based on the configured time intervals and update the configuration table accordingly. The logs will be generated in the `logs` directory as specified in the `loggers.ini` file.

## Contributing

Feel free to contribute to the project by submitting pull requests, reporting issues, or suggesting new features.

## License

This project is licensed under the MIT License - see the `LICENSE` file for details.

---

Please note that the provided code snippets and configuration files are placeholders and should be replaced with actual code and configuration details specific to your project.