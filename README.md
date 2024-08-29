# QualityChecker aka Колян

QualityChecker is a utility that allows for quick checks of ODS and STG tables for typical errors. As of August 18, 2024, it supports Vertica and Greenplum databases.

Supported checks:

1. Duplicates by keys
2. Completely empty columns (NULL or '')
3. Text fields whose length has reached the maximum
4. Presence of non-UTF-8 characters
5. Maximum `tech_load_ts` in ODS
6. Increment correctness check (only tested for Vertica)
7. Statistics of the most frequently occurring values in a field and their share of the total
8. Statistics of text field lengths: varchar of the largest value and maximum length
9. Segmentation
10. Number of rows in STG
11. Number of rows in ODS
12. Number of unique keys without `tech_load_ts`
13. Duplicates by key in STG
14. Maximum `tech_load_ts` in STG

## Table of Contents

- [Getting Started](#getting-started)
  - [Prerequisites](#prerequisites)
  - [Installation](#installation )
- [Usage](#usage)
- [Project Structure](#project-structure)
- [Contributing](#contributing)
- [License](#license)

## Getting Started

These instructions will get you a copy of the project up and running on your local machine for development and testing purposes.

### Prerequisites

- Python 3.x
- Poetry (for managing dependencies, optional)

### Installation

#### Installation (with poetry)

1. Clone the repository:

   `````bash
   git clone https://github.com/buriiick/QualityChecker
   `````
2. Change into the project directory:

   ````bash
   cd QualityChecker/QualityChecker
   ````
3. Install the dependencies:

   ````bash
   poetry install
   ````

#### Installation (with pip)

1. **Clone the repository:**

   ```bash
   git clone https://github.com/buriiick/QualityChecker
   ```
2. **Change into the project directory:**

   ```bash
   cd QualityChecker/QualityChecker
   ```
3. **Install the dependencies:**

   ```bash
   pip install -r requirements.txt
   ```

### Usage

1. Make sure you are in the project directory:

   ````bash
   cd QualityChecker
   ```

   ````
2. (optional) In the file `get_tables_sql_query.sql`, specify the query that retrieves the schema and table names from the metadata.
3. Run the main program:

   ````bash
   python QualityChecker/main.py
   ```

   This will run the `main.py` file located in the `src/` directory.

   ````
4. Next, answer a few questions in the console.
5. Reports are available in the `report` directory.

## Project Structure

```
├── CONTRIBUTING.md
├── LICENSE
├── QualityChecker
│  ├── checks.py
│  ├── conf.py
│  ├── config.json
│  ├── DataQuality.py
│  ├── get_tables_sql_query.sql
│  ├── main.py
│  ├── poetry.lock
│  ├── pyproject.toml
│  ├── reports
│  │  ├── # your reports
│  ├── requirements.txt
│  ├── sql
│  │  ├── DQ
│  │  │  ├── ansi
│  │  │  │  ├── check_bussines_key_counts.sql
│  │  │  │  ├── check_max_length.sql
│  │  │  │  ├── check_max_tech_load_ts.sql
│  │  │  │  ├── check_not_nulls_columns.sql
│  │  │  │  ├── check_pk_doubles.sql
│  │  │  │  └── check_row_count.sql
│  │  │  ├── greenplum
│  │  │  │  ├── check_max_length.sql
│  │  │  │  ├── check_not_utf8.sql
│  │  │  │  ├── check_segmentation.sql
│  │  │  │  ├── select_columns_length_statistics.sql
│  │  │  │  └── select_most_consistent_value.sql
│  │  │  └── vertica
│  │  │     ├── check_insert_new_rows_with_deleted.sql
│  │  │     ├── check_insert_new_rows_wo_deleted.sql
│  │  │     ├── check_max_length.sql
│  │  │     ├── check_not_utf8.sql
│  │  │     ├── check_segmentation.sql
│  │  │     ├── select_columns_length_statistics.sql
│  │  │     └── select_most_consistent_value.sql
│  │  └── work_with_meta
│  │     ├── greenplum
│  │     │  ├── select_all_columns.sql
│  │     │  ├── select_business_columns.sql
│  │     │  └── select_primary_key_columns.sql
│  │     └── vertica
│  │        ├── generate_script.sql
│  │        ├── select_all_columns.sql
│  │        ├── select_business_columns.sql
│  │        └── select_primary_key_columns.sql
│  └── utils
│     ├── databaseTools.py
│     └── utils.py
└── README.md
```

## Contributing

Please read [CONTRIBUTING.md](CONTRIBUTING.md) for details on our code of conduct, and the process for submitting pull requests to us.

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
