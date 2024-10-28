# Jaffle Shop - dbt Project

🥪🦘 An open source sandbox project exploring dbt workflows via a fictional sandwich shop's data.

## Project Overview

This project uses dbt (data build tool) to transform raw data from a fictional sandwich shop into meaningful insights. It demonstrates best practices in data modeling, testing, and documentation using dbt.

## Getting Started

1. Clone this repository
2. Install dbt and configure your data warehouse connection
3. Run `dbt deps` to install dependencies
4. Run `dbt run` to build the models
5. Run `dbt test` to execute tests
6. Run `dbt docs generate` and `dbt docs serve` to view documentation

## Project Structure

- `models/`: Contains SQL models
  - `staging/`: Models that clean and standardize source data
  - `marts/`: Business-level models built on staging models
- `tests/`: Custom data tests
- `macros/`: Reusable SQL snippets
- `analysis/`: Ad-hoc analytical queries
- `docs/`: Additional documentation

## Contributing

We welcome contributions! Please see our [contributing guidelines](CONTRIBUTING.md) for more details.

## License

This project is licensed under the [MIT License](LICENSE).