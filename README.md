# Data Analysis Technical Exercises

This repository contains two main technical exercises:
1. **Backend Engineer Technical Exercise**: Implemented in both Python and Java.
2. **NLP/LLM Exercise**: Implemented in Python.

Below, you will find details for each exercise, including requirements, instructions for running the code, and descriptions of the implementations.

## Table of Contents
- [Backend Engineer Technical Exercise](#backend-engineer-technical-exercise)
    - [Python Solution](#python-solution)
    - [Java Solution](#java-solution)
- [NLP/LLM Exercise](#nlpllm-exercise)
    - [Python Solution](#nlp-python-solution)
- [Setup and Requirements](#setup-and-requirements)
- [Directory Structure](#directory-structure)

## Backend Engineer Technical Exercise

### Python Solution
The Python solution for the backend engineer exercise can be found in `backend_technical_exercise.py`. This solution performs the following:
- Loads CSV files in chunks using Dask to handle large datasets.
- Analyzes column types and compares columns across two files to find similarities.
- Uses multiprocessing for efficient column comparison.

#### Running the Python Solution
To execute the Python solution, run:

```sh
python backend_technical_exercise.py
```

Make sure to install the necessary dependencies first using `requirements.txt` (see the [Setup and Requirements](#setup-and-requirements) section below).

### Java Solution
The Java solution can be found in `CsvColumnComparator.java` under `src/main/java/org/example/`. It performs similar operations as the Python version:
- Loads CSV files and determines column types.
- Compares columns across two files to find similarities based on pre-defined criteria.
- Utilizes concurrency with Java's ExecutorService for efficient processing.

#### Running the Java Solution
To compile and run the Java solution:

1. Ensure you have Apache Maven installed.
2. Compile the Java code:

   ```sh
   mvn compile
   ```

3. Run the Java program:

   ```sh
   mvn exec:java -Dexec.mainClass="org.example.CsvColumnComparator"
   ```

Dependencies for the Java solution are managed in `pom.xml`.

## NLP/LLM Exercise

### NLP Python Solution
The NLP/LLM exercise is implemented in `nlp_llm_exercise.py`. This script performs natural language processing tasks using relevant libraries and techniques for text analysis.

#### Running the NLP/LLM Solution
To execute the NLP solution:

```sh
python nlp_llm_exercise.py
```

Ensure all dependencies are installed as per the `requirements.txt` file.

## Setup and Requirements

### Python Setup
To set up the Python environment for the exercises:

1. Ensure you have Python 3 installed.
2. Install the required dependencies:

   ```sh
   pip install -r requirements.txt
   ```

### Java Setup
To set up the Java environment:

1. Ensure you have JDK 8 or later installed.
2. Install Apache Maven for dependency management.
3. Dependencies are defined in the `pom.xml` file located in the root of the project.

## Directory Structure

The project is organized as follows:

```
.
├── README.md                           # Documentation for the project
├── SampleData1.csv                     # Sample dataset for testing
├── SampleData2.csv                     # Sample dataset for testing
├── backend_technical_exercise.py       # Python solution for Backend Engineer Technical Exercise
├── data_analysis.ipynb                 # Original notebook containing both the excercise
├── nlp_llm_exercise.py                 # Python solution for NLP/LLM Exercise
├── pom.xml                             # Maven configuration for Java dependencies
├── requirements.txt                    # Python dependencies
├── src
│   └── main
│       └── java
│           └── org
│               └── example
│                   ├── CSVComparator.java       # Additional utility Java file
│                   └── CsvColumnComparator.java # Java solution for Backend Engineer Technical Exercise

```
