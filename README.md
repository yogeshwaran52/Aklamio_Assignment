# Aklamio Data Processing Assignment

## Overview

This project is a data processing application that reads JSON data and processes it either in batch mode or in streaming mode. The processed data is stored in a PostgreSQL database. The application performs data cleaning, metrics calculation, and logging of both successful and failed data entries.

## Features/Process

- **Batch Processing**: Processes the entire JSON file at once.
- **Streaming Processing**: Processes one line of JSON data at a time, for large files.
- **Data Cleaning**: Validates and cleans data based on specific criteria. Check if the field event_type is nul, empty or with Value "EMPTY_VALUE" and Invalid date format.
- **Metrics Calculation**: Computes page loads, clicks, unique user clicks, and click-through-rate.
- **PostgreSQL Integration**: Stores cleaned data and metrics in a PostgreSQL database. 

## Assumption

In batch process, I assumed there will be some duplicates. In Streaming Process, I assumed there wont be any duplicates

## Prerequisites

- [Docker](https://www.docker.com/get-started)
- PostgreSQL running in Docker

## Getting Started

### Clone the Repository

First, clone the repository containing the project files:

```bash
git clone [https://github.com/yogeshwaran52/Aklamio_Assignment.git](https://github.com/yogeshwaran52/Aklamio_Assignment.git)
cd Aklamio_Assignment
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
TABLES:
1) cleaned_data - Contains the data which is cleaned based on specific condition mentioned in Features.
2) failed_data - Contains the data which failed the validation.
3) hourly_aggregates - Contains the metrics calculation per customer. 

```bash
-- View cleaned data
SELECT * FROM cleaned_data;

-- View failed data
SELECT * FROM failed_data;

-- View hourly aggregates
SELECT * FROM hourly_aggregates;
SELECT customer_id, hour, page_loads, clicks, unique_user_clicks, click_through_rate FROM hourly_aggregates;


Once done you can exit with command "\q"
```
