# Pacoin: Cryptocurrency Trading Advisory Platform
<p align="center">
  <img src="https://github.com/user-attachments/assets/d8e87020-bb55-4270-98d7-db03f9da14fe" alt="PACOIN">
</p>

## Overview
Pacoin is a comprehensive Big Data architecture project designed to assist a major bank in establishing a new department focused on advising clients in cryptocurrency trading. The project leverages Amazon Web Services (AWS) to create a robust system capable of handling both batch and real-time data processing. The architecture is tailored to ingest, store, analyze, and visualize data from TradingView, enabling the bank to provide actionable insights and real-time monitoring for cryptocurrency markets.

## Project Objectives
The primary goal of Pacoin is to design and implement a scalable Big Data architecture on AWS that supports:
1. **Data Acquisition**:
   - Extraction of historical data for batch processing.
   - Real-time data ingestion using AWS Lambda, Amazon Kinesis, and Amazon MSK.
2. **Data Storage**:
   - Batch data storage in Amazon S3.
   - Real-time data storage in Amazon DynamoDB or Amazon RDS.
3. **Data Analysis**:
   - Batch processing using Amazon EMR or AWS Glue.
   - Real-time analysis with Amazon Kinesis Data Analytics or AWS Lambda.
4. **Data Visualization**:
   - Interactive dashboards using Amazon QuickSight or Amazon Managed Grafana.
   - Generation of reports and alerts based on analyzed data.

## Use Cases
1. **Market Trend Analysis**:
   - Process historical data to identify patterns and trends in cryptocurrency prices.
   - Visualize these trends in interactive dashboards.
2. **Real-Time Monitoring**:
   - Implement real-time alerts for significant price changes.
   - Display real-time data on interactive dashboards.
3. **Report Generation**:
   - Create detailed reports on market performance.

## Proposed Architecture
The architecture is designed to handle end-to-end data processing and analysis, ensuring scalability, reliability, and efficiency. Key components include:
- **Data Ingestion**: AWS Lambda, Amazon Kinesis, Amazon MSK.
- **Data Storage**: Amazon S3, Amazon DynamoDB, Amazon RDS.
- **Data Processing**: Amazon EMR, AWS Glue, Amazon Kinesis Data Analytics.
- **Data Visualization**: Amazon QuickSight, Amazon Managed Grafana.

## Methodology
The project follows the Scrum methodology to ensure an agile and collaborative workflow. Key Scrum practices include:
- **Sprint Planning**: Define tasks and goals for each sprint.
- **Daily Scrum**: Daily stand-up meetings to track progress.
- **Sprint Review**: Evaluate the work completed during the sprint.
- **Sprint Retrospective**: Reflect on the sprint and identify areas for improvement.

Teams consist of 3 members, each representing a consulting firm specializing in cryptocurrency trading.

## Final Deliverable
At the end of the project, each consulting team will present their solution to the bank's project supervisors. The presentation will demonstrate the implemented architecture, its functionality, and its ability to meet the bank's objectives.

## Repository Structure
pacoin/
├── data/ # Contains sample datasets and processed data
├── scripts/ # Scripts for data extraction, processing, and analysis
├── docs/ # Documentation and project reports
├── architecture/ # Diagrams and descriptions of the proposed architecture
├── README.md # Project overview and instructions
└── requirements.txt # Python dependencies for the project


## Getting Started
1. **Clone the Repository**:
   git clone https://github.com/your-username/pacoin.git
   cd pacoin
Set Up AWS Environment:
Ensure you have access to the required AWS services.
Configure AWS credentials using the AWS CLI.
Install Dependencies:
bash
Copy
pip install -r requirements.txt
Run Data Processing Scripts:
Execute the provided scripts for data ingestion, processing, and visualization.
Contributing

Contributions to Pacoin are welcome! Please follow these steps:

Fork the repository.
Create a new branch for your feature or bugfix.
Commit your changes and push to the branch.
Submit a pull request with a detailed description of your changes.

Contact

For any questions or inquiries, please contact the project team at 202212847@alu.comillas.edu

Pacoin is a project designed to empower financial institutions with cutting-edge tools for cryptocurrency trading advisory. By leveraging AWS and agile methodologies, Pacoin delivers a scalable and efficient solution for market analysis and real-time monitoring.
