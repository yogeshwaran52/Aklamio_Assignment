# Aklamio Data Processing Assignment

## Overview

This project is a data processing application that reads JSON data and processes it either in batch mode or in streaming mode. The processed data is stored in a PostgreSQL database. The application performs data cleaning, metrics calculation, and logging of both successful and failed data entries.

## Features

- **Batch Processing**: Processes the entire JSON file at once.
- **Streaming Processing**: Processes one line of JSON data at a time, suitable for large files or real-time data.
- **Data Cleaning**: Validates and cleans data based on specific criteria.
- **Metrics Calculation**: Computes page loads, clicks, unique user clicks, and click-through rates.
- **PostgreSQL Integration**: Stores cleaned data and metrics in a PostgreSQL database.
- **Logging**: Logs application activities and errors for monitoring and debugging.

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- [Docker Compose](https://docs.docker.com/compose/install/)
- PostgreSQL running in Docker

## Getting Started

### Clone the Repository

First, clone the repository containing the project files:

```bash
git clone <repository-url>
cd <repository-directory>
```

### Build and Start the Application
Use the following command to build the Docker images and start the application:

```bash
docker-compose up --build -d
```

### Run the Application
You can run the application in either batch or stream mode:

### Batch Mode
To process the JSON file in batch mode:

```bash 
docker-compose run app python main.py batch
```

### Streaming Mode
To process the JSON file in streaming mode:
```bash 
docker-compose run app python main.py stream
```

### Access PostgreSQL Database
Once the processing is complete, you can access the PostgreSQL database to view the processed data:
```bash
docker-compose exec postgres psql -U postgres -d aklamio
```

### Querying the Data
Inside the PostgreSQL prompt, you can run the following SQL queries to inspect the processed data:

```bash
-- View cleaned data
SELECT * FROM cleaned_data;

-- View failed data
SELECT * FROM failed_data;

-- View hourly aggregates
SELECT * FROM hourly_aggregates;

Once done you can exit with command "\q"
```